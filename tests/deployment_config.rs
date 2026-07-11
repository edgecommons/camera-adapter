//! Deployment examples are part of the component contract: a configuration guide that cannot
//! pass the component's strict parser is worse than no guide at all.

use camera_adapter::config::AdapterConfig;

fn validate_example(raw: &str) {
    let value: serde_json::Value = serde_json::from_str(raw).expect("example must be JSON");
    let core = edgecommons::config::Config::from_value(
        camera_adapter::COMPONENT_NAME,
        "deployment-test",
        value,
    )
    .expect("example must satisfy the core schema");
    AdapterConfig::from_core_initial(&core).expect("example must satisfy the adapter schema");
}

#[test]
fn docker_simulator_config_is_a_valid_initial_configuration() {
    validate_example(include_str!("../deploy/docker/simulator-config.json"));
}

#[test]
fn kubernetes_configmap_embeds_a_valid_initial_configuration() {
    let document = include_str!("../k8s/configmap.yaml");
    let marker = "  config.json: |-\n";
    let (_, body) = document
        .split_once(marker)
        .expect("ConfigMap must contain config.json");
    let json = body
        .lines()
        .map_while(|line| line.strip_prefix("    "))
        .collect::<Vec<_>>()
        .join("\n");
    validate_example(&json);
}
