//! Camera Adapter process entry point.
//!
//! The binary uses the standard EdgeCommons CLI contract and starts not-ready while
//! component-owned configuration, durable state, storage, commands, and supervisors initialize.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use camera_adapter::{
    COMPONENT_NAME,
    backend::BackendRuntimeContext,
    config::AdapterConfig,
    runtime::{
        RuntimeCommandRouter, RuntimeConfigListener, RuntimeReadiness, RuntimeServices,
        prepare_startup_resources, validate_configuration_candidate_with_credentials,
    },
};
use edgecommons::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    camera_adapter::supervisor::install_sanitized_panic_hook();
    let router = RuntimeCommandRouter::new();
    // Core credential services are immutable after their initial construction. The validator
    // needs this shared fact to veto a later ONVIF secret reference before its config generation
    // can commit. Initial validation is still configuration-only; startup verifies the service
    // immediately after `build` below.
    let credential_service_available = Arc::new(AtomicBool::new(false));
    let configuration_validator = {
        let credential_service_available = Arc::clone(&credential_service_available);
        move |candidate, current, phase| {
            validate_configuration_candidate_with_credentials(
                candidate,
                current,
                phase,
                credential_service_available.load(Ordering::Acquire),
            )
        }
    };
    let gg = Arc::new(
        EdgeCommonsBuilder::new(COMPONENT_NAME)
            .args(std::env::args_os())
            .initial_ready(false)
            .configuration_validator("camera-adapter", configuration_validator)?
            .configure_commands({
                let router = Arc::clone(&router);
                move |inbox| router.register(inbox)
            })
            .build()
            .await?,
    );

    let loaded = AdapterConfig::from_core_initial(&gg.config())?;
    for issue in &loaded.skipped {
        tracing::warn!(
            instance = ?issue.instance,
            path = %issue.path,
            message = %issue.message,
            "skipping invalid camera instance during initial startup"
        );
    }
    tracing::info!(
        component = gg.component_name(),
        thing = %gg.config().thing_name,
        cameras = loaded.config.instances.len(),
        "camera-adapter configuration accepted"
    );
    let credential_service = gg.credentials();
    credential_service_available.store(credential_service.is_some(), Ordering::Release);
    let backend_context = BackendRuntimeContext::new(credential_service);
    backend_context.validate_config(&loaded.config)?;

    // State/catalog/output startup gates run before any camera connection. The complete runtime
    // installs itself into `router` only after recovery and supervisor construction; until then
    // all camera verbs reply with a stable startup-unavailable error and readiness remains false.
    let resources = prepare_startup_resources(&loaded.config, gg.args().platform).await?;
    tracing::info!(
        state_directory = %resources.state_directory.display(),
        catalog = %resources.catalog.database_path().display(),
        output_root = %resources.storage.canonical_root().display(),
        "camera-adapter durable startup gates passed"
    );

    let mut apps = BTreeMap::new();
    let mut events = BTreeMap::new();
    for camera in &loaded.config.instances {
        let instance = gg.instance(&camera.id)?;
        apps.insert(camera.id.clone(), Arc::new(instance.app()));
        events.insert(camera.id.clone(), instance.events());
    }
    let readiness = {
        let gg = Arc::clone(&gg);
        RuntimeReadiness::new(Arc::new(move |ready| gg.set_ready(ready)))
    };
    let runtime = camera_adapter::runtime::CameraRuntime::start(
        loaded.config,
        resources,
        RuntimeServices {
            apps,
            events,
            outbox_events: gg.events(),
            readiness: readiness.clone(),
            backend_context,
            messaging: gg.messaging()?,
        },
    )
    .await?;
    router.install(runtime.clone())?;
    let app_factory = {
        let gg = Arc::clone(&gg);
        Arc::new(move |instance: &str| gg.instance(instance).map(|handle| Arc::new(handle.app())))
    };
    let events_factory = {
        let gg = Arc::clone(&gg);
        Arc::new(move |instance: &str| gg.instance(instance).map(|handle| handle.events()))
    };
    gg.add_config_change_listener(Arc::new(RuntimeConfigListener::new(
        Arc::downgrade(&runtime),
        app_factory,
        events_factory,
    )));
    readiness.complete_startup();
    tracing::info!("camera-adapter runtime installed and ready");
    gg.shutdown_signal().await;
    router.begin_shutdown();
    runtime.shutdown().await;
    Ok(())
}
