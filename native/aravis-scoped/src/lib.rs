//! Audited safe scope for Aravis' process-global GigE discovery-interface setting.
//!
//! Aravis 0.8.36 exposes this setting only through C symbols that are newer than the pinned
//! `aravis-sys` bindings. This crate contains the entire unsafe boundary. Callers receive only a
//! closure-scoped token; raw pointers and symbols never cross the API.

#![deny(unsafe_op_in_unsafe_fn)]

use std::ffi::{CStr, CString, c_char};
use std::fmt;
use std::sync::{Mutex, PoisonError};

static SCOPE_LOCK: Mutex<()> = Mutex::new(());
static NATIVE_ABI: NativeAbi = NativeAbi;

unsafe extern "C" {
    fn arv_gv_interface_set_discovery_interface_name(discovery_interface: *const c_char);
    fn arv_gv_interface_dup_discovery_interface_name() -> *mut c_char;
    fn arv_get_device_serial_nbr(index: u32) -> *const c_char;
}

/// Validation failure before or while copying a string from the native ABI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeError {
    /// A configured interface contains an interior NUL and cannot be represented to C.
    InteriorNul,
    /// Aravis returned a device serial that is not valid UTF-8.
    InvalidUtf8,
}

impl fmt::Display for ScopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InteriorNul => "discovery interface contains an interior NUL",
            Self::InvalidUtf8 => "Aravis returned a non-UTF-8 device serial",
        })
    }
}

impl std::error::Error for ScopeError {}

/// Token proving the caller holds the process-wide Aravis discovery scope.
pub struct ScopedDiscovery {
    _private: (),
}

impl ScopedDiscovery {
    /// Copies the serial number at the current Aravis device-list index.
    ///
    /// The pointer returned by Aravis is borrowed from its global device list, so copying is only
    /// permitted while the same scope lock that protected list creation is still held.
    pub fn serial_number(&self, index: u32) -> Result<Option<String>, ScopeError> {
        NATIVE_ABI.serial_number(index)
    }
}

/// Runs `operation` while Aravis discovery and direct camera opening are restricted to one OS
/// network interface.
///
/// `None` restores Aravis' legacy all-interface behavior and is intended only for callers that have
/// separately disabled GigE discovery. The previous setting is restored on normal return and
/// unwinding. The process-wide lock must cover every set/list/open sequence because
/// `arv_camera_new` consults the same singleton as `arv_update_device_list`.
pub fn with_discovery_interface<T>(
    interface: Option<&str>,
    operation: impl FnOnce(&ScopedDiscovery) -> T,
) -> Result<T, ScopeError> {
    let interface = interface
        .map(CString::new)
        .transpose()
        .map_err(|_| ScopeError::InteriorNul)?;
    let _lock = SCOPE_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    Ok(with_scope_abi(&NATIVE_ABI, interface.as_deref(), || {
        operation(&ScopedDiscovery { _private: () })
    }))
}

trait ScopeAbi {
    fn duplicate_interface(&self) -> Option<CString>;
    fn set_interface(&self, interface: Option<&CStr>);
}

struct RestoreScope<'a, A: ScopeAbi> {
    abi: &'a A,
    previous: Option<CString>,
}

impl<A: ScopeAbi> Drop for RestoreScope<'_, A> {
    fn drop(&mut self) {
        self.abi.set_interface(self.previous.as_deref());
    }
}

fn with_scope_abi<A: ScopeAbi, T>(
    abi: &A,
    interface: Option<&CStr>,
    operation: impl FnOnce() -> T,
) -> T {
    let previous = abi.duplicate_interface();
    abi.set_interface(interface);
    let _restore = RestoreScope { abi, previous };
    operation()
}

struct NativeAbi;

impl NativeAbi {
    fn serial_number(&self, index: u32) -> Result<Option<String>, ScopeError> {
        // SAFETY: Aravis owns the returned NUL-terminated string. The public token guarantees the
        // global device list cannot be refreshed by another adapter operation while it is copied.
        let pointer = unsafe { arv_get_device_serial_nbr(index) };
        if pointer.is_null() {
            return Ok(None);
        }
        // SAFETY: The Aravis API contract returns a valid borrowed C string for a live list index.
        let serial = unsafe { CStr::from_ptr(pointer) }
            .to_str()
            .map_err(|_| ScopeError::InvalidUtf8)?;
        Ok(Some(serial.to_owned()))
    }
}

impl ScopeAbi for NativeAbi {
    fn duplicate_interface(&self) -> Option<CString> {
        // SAFETY: The symbol returns either NULL or a GLib-allocated NUL-terminated duplicate.
        let pointer = unsafe { arv_gv_interface_dup_discovery_interface_name() };
        if pointer.is_null() {
            return None;
        }
        // Copy before releasing with the allocator documented by the Aravis API.
        // SAFETY: The pointer remains valid until `g_free` below.
        let owned = unsafe { CStr::from_ptr(pointer) }.to_owned();
        // SAFETY: Aravis allocated this duplicate with GLib and transfers full ownership.
        unsafe { glib_sys::g_free(pointer.cast()) };
        Some(owned)
    }

    fn set_interface(&self, interface: Option<&CStr>) {
        let pointer = interface.map_or(std::ptr::null(), CStr::as_ptr);
        // SAFETY: The pointer is NULL or a valid C string for the duration of the call. Aravis
        // duplicates the value into its process-global singleton.
        unsafe { arv_gv_interface_set_discovery_interface_name(pointer) };
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct MockAbi {
        current: Mutex<Option<CString>>,
    }

    impl ScopeAbi for MockAbi {
        fn duplicate_interface(&self) -> Option<CString> {
            self.current.lock().unwrap().clone()
        }

        fn set_interface(&self, interface: Option<&CStr>) {
            *self.current.lock().unwrap() = interface.map(CStr::to_owned);
        }
    }

    #[test]
    fn scope_restores_previous_value_on_return_and_unwind() {
        let abi = MockAbi::default();
        abi.set_interface(Some(c"eth-old"));
        let value = with_scope_abi(&abi, Some(c"eth-new"), || {
            assert_eq!(abi.duplicate_interface().as_deref(), Some(c"eth-new"));
            42
        });
        assert_eq!(value, 42);
        assert_eq!(abi.duplicate_interface().as_deref(), Some(c"eth-old"));

        let panic = catch_unwind(AssertUnwindSafe(|| {
            with_scope_abi(&abi, Some(c"eth-panic"), || panic!("injected"));
        }));
        assert!(panic.is_err());
        assert_eq!(abi.duplicate_interface().as_deref(), Some(c"eth-old"));
    }
}
