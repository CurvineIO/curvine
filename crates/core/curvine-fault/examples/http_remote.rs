// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Remote fault injection over an embedded `fault_router` plus a client
//! driving the full configure / trigger / observe / cleanup lifecycle.
//!
//! Hosts that already expose an Axum web server (Curvine Master/Worker) mount
//! the same router. This example owns a temporary listener to demonstrate how
//! a host embeds the router.
//!
//! ```bash
//! cargo run -p curvine-fault --example http_remote \
//!   --features http-server,http-client
//! ```

use curvine_fault::{
    fault_point, ConfigureFaultRule, FaultAction, FaultController, FaultErrorSpec, FaultHttpConfig,
    FaultHttpControl, FaultHttpController,
};

fn handle_request(req_id: i64) -> Result<(), String> {
    fault_point! {
        sync,
        name: "agent.request.before_handle",
        description: "Before an agent request is handled",
        context: { "req_id" => req_id },
        return_error: |fault| Err(fault.message),
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const TOKEN: &str = "example-secret";
    const TOKEN_ENV: &str = "CURVINE_FAULT_HTTP_REMOTE_EXAMPLE_TOKEN";

    std::env::set_var(TOKEN_ENV, TOKEN);
    let fault_http = FaultHttpControl::from_env(&FaultHttpConfig {
        enabled: true,
        auth_token_env: TOKEN_ENV.to_string(),
    })?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(listener, fault_http.mount(axum::Router::new()))
            .await
            .unwrap();
    });
    println!("fault API listening on http://{address} (bearer token: {TOKEN})");
    println!("try: curl -H 'Authorization: Bearer {TOKEN}' http://{address}/api/v1/debug/faults");

    // A remote controller (any machine that can reach the port) installs a rule.
    let controller = FaultHttpController::new(format!("http://{address}"), TOKEN)?;
    controller
        .configure(
            "fail-req-42",
            ConfigureFaultRule {
                point: "agent.request.before_handle".to_string(),
                matcher: {
                    let mut matcher = curvine_fault::FaultMatcher::default();
                    matcher.insert("req_id", 42_i64);
                    matcher
                },
                action: FaultAction::ReturnError {
                    error: FaultErrorSpec {
                        message: "injected by remote controller".to_string(),
                    },
                },
                ..Default::default()
            },
        )
        .await?;

    assert!(handle_request(1).is_ok());
    assert_eq!(
        handle_request(42).unwrap_err(),
        "injected by remote controller"
    );

    let status = controller.status().await?;
    println!("remote status: rules={}", status.rules.len());
    controller.clear().await?;
    server.abort();
    Ok(())
}
