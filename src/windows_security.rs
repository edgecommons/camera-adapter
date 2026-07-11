//! Windows DACL enforcement for adapter-owned durable state.
//!
//! The state database contains command-idempotency and pending result material, so it must not
//! inherit broad `ProgramData` permissions.  This module deliberately applies ACLs to *open
//! handles* instead of paths: a path could be swapped for a reparse point between validation and
//! ACL application.  The resulting protected DACL permits only the running service identity plus
//! `SYSTEM` and local Administrators.  Output files are not handled here because deployments may
//! intentionally grant a separate file-replicator identity read access to them.

use std::io;
use std::os::windows::io::AsRawHandle;

use windows_permissions::{
    LocalBox, SecurityDescriptor,
    constants::{SeObjectType::SE_FILE_OBJECT, SecurityInformation},
    utilities::current_process_sid,
    wrappers::SetSecurityInfo,
};

/// Applies the restricted, protected state DACL to an already-open file or directory handle.
///
/// The descriptor has no `Users`, `Authenticated Users`, or inherited grants.  The service SID is
/// resolved from the effective process token rather than from a configurable account-name string.
/// Failure is fatal to startup/write setup because continuing with inherited `ProgramData` access
/// would violate the state confidentiality/integrity contract.
pub fn restrict_state_handle<H: AsRawHandle>(handle: &mut H) -> io::Result<()> {
    let service_sid = current_process_sid()?;
    let descriptor: LocalBox<SecurityDescriptor> =
        format!("D:P(A;;FA;;;{service_sid})(A;;FA;;;SY)(A;;FA;;;BA)").parse()?;
    let dacl = descriptor
        .dacl()
        .ok_or_else(|| io::Error::other("restricted state DACL did not contain a DACL"))?;
    SetSecurityInfo(
        handle,
        SE_FILE_OBJECT,
        SecurityInformation::Dacl | SecurityInformation::ProtectedDacl,
        None,
        None,
        Some(dacl),
        None,
    )
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;

    use windows_permissions::{
        constants::{SeObjectType::SE_FILE_OBJECT, SecurityInformation},
        utilities::current_process_sid,
        wrappers::{ConvertSecurityDescriptorToStringSecurityDescriptor, GetSecurityInfo},
    };

    use super::restrict_state_handle;

    #[test]
    fn restricted_state_dacl_is_protected_and_excludes_broad_user_grants() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("state.lock");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .access_mode(0xC004_0000) // GENERIC_READ | GENERIC_WRITE | WRITE_DAC
            .open(path)
            .expect("state file");
        restrict_state_handle(&mut file).expect("restrict DACL");
        let descriptor =
            GetSecurityInfo(&file, SE_FILE_OBJECT, SecurityInformation::Dacl).expect("read DACL");
        let sddl = ConvertSecurityDescriptorToStringSecurityDescriptor(
            &descriptor,
            SecurityInformation::Dacl,
        )
        .expect("render DACL")
        .to_string_lossy()
        .to_ascii_uppercase();
        assert!(sddl.starts_with("D:P"));
        assert!(!sddl.contains(";;;WD)"));
        assert!(!sddl.contains(";;;AU)"));
        assert!(
            sddl.contains(&current_process_sid().expect("service SID").to_string()),
            "restricted DACL must retain the running service identity: {sddl}"
        );
        drop(file);
        assert!(std::fs::read(directory.path().join("state.lock")).is_ok());
        assert!(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(directory.path().join("state.lock"))
                .is_ok(),
            "the running service identity must retain ordinary read/write access"
        );
    }
}
