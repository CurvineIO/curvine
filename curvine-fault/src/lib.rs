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

mod builder;
mod catalog;
mod error;
mod http_control;
mod model;
mod runtime;

#[cfg(feature = "http-client")]
mod controller;

#[cfg(feature = "http-server")]
mod http_server;

pub use builder::FaultRuleBuilder;
#[cfg(feature = "http-client")]
pub use controller::{
    FaultController, FaultHttpController, FaultLocalController, FaultTestSession,
};
#[cfg(feature = "http-client")]
pub use error::FaultControlError;
pub use error::FaultHttpError;
pub use error::FaultRuntimeError;
pub use http_control::FaultHttpControl;
#[cfg(feature = "http-server")]
pub use http_server::{fault_router, FaultHttpAuth};
pub use model::*;
pub use runtime::FaultRuntime;

#[doc(hidden)]
pub use linkme as __linkme;
#[doc(hidden)]
pub use linkme::distributed_slice as __distributed_slice;

#[linkme::distributed_slice]
#[doc(hidden)]
pub static FAULT_POINT_REGISTRATIONS: [FaultPointRegistration];

#[doc(hidden)]
pub fn execute_sync_action(decision: Option<FaultDecision>) -> Option<FaultErrorSpec> {
    match decision {
        Some(FaultDecision {
            action: FaultAction::Delay { duration_ms },
            ..
        }) => {
            std::thread::sleep(std::time::Duration::from_millis(duration_ms));
            None
        }
        decision => execute_non_delay_action(decision),
    }
}

#[doc(hidden)]
pub async fn execute_async_action(decision: Option<FaultDecision>) -> Option<FaultErrorSpec> {
    match decision {
        Some(FaultDecision {
            action: FaultAction::Delay { duration_ms },
            ..
        }) => {
            tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;
            None
        }
        decision => execute_non_delay_action(decision),
    }
}

fn execute_non_delay_action(decision: Option<FaultDecision>) -> Option<FaultErrorSpec> {
    let decision = decision?;
    match decision.action {
        FaultAction::Record => None,
        FaultAction::ReturnError { error } => Some(error),
        FaultAction::Crash => std::process::abort(),
        FaultAction::Delay { .. } => unreachable!("delay must be handled by the execution model"),
    }
}

#[doc(hidden)]
pub fn invoke_return_handler<R, F>(handler: F, error: FaultErrorSpec) -> R
where
    F: FnOnce(FaultErrorSpec) -> R,
{
    handler(error)
}

#[macro_export]
#[doc(hidden)]
macro_rules! __fault_evaluate {
    ($runtime:expr, $point:expr, $context:expr) => {
        ($runtime).evaluate($point, $context)
    };
    ($point:expr, $context:expr) => {
        $crate::FaultRuntime::process().evaluate($point, $context)
    };
}

#[macro_export]
macro_rules! fault_context {
    ($($key:literal => $value:expr),* $(,)?) => {{
        let mut __context = $crate::FaultContext::default();
        $(__context.insert($key, $value);)*
        __context
    }};
}

#[cfg(feature = "fault-injection")]
#[macro_export]
#[doc(hidden)]
macro_rules! __fault_point_spec {
    (
        name: $name:literal,
        description: $description:literal,
        execution: $execution:expr,
        matchers: [$($matcher:literal),* $(,)?],
        actions: [$($action:expr),* $(,)?] $(,)?
    ) => {{
        const _: () = assert!(
            $crate::point_name_is_valid($name),
            "fault point name must not be empty",
        );
        static __POINT: $crate::FaultPointSpec = $crate::FaultPointSpec {
            name: $name,
            description: $description,
            execution: $execution,
            matchers: &[$($matcher),*],
            actions: &[$($action),*],
        };
        #[$crate::__distributed_slice($crate::FAULT_POINT_REGISTRATIONS)]
        #[linkme(crate = $crate::__linkme)]
        static __REGISTRATION: $crate::FaultPointRegistration = $crate::FaultPointRegistration {
            point: &__POINT,
            file: file!(),
            line: line!(),
        };
        &__POINT
    }};
}

#[cfg(feature = "fault-injection")]
#[macro_export]
#[doc(hidden)]
macro_rules! __fault_point_prepare {
    (
        execution: $execution:expr,
        actions: [$($action:expr),* $(,)?],
        name: $name:literal,
        description: $description:literal,
        $(runtime: $runtime:expr,)?
        context: {$($key:literal => $value:expr),* $(,)?} $(,)?
    ) => {{
        let __point = $crate::__fault_point_spec! {
            name: $name,
            description: $description,
            execution: $execution,
            matchers: [$($key),*],
            actions: [$($action),*],
        };
        let __context = $crate::fault_context! {$($key => $value),*};
        let __decision =
            $crate::__fault_evaluate!($($runtime,)? __point, &__context);
        (__point, __decision)
    }};
}

/// Declares, registers, and evaluates one fault point.
///
/// # Sync vs async
///
/// - A `sync` point executes `Delay` with `std::thread::sleep` on the caller
///   thread. On Curvine Master/Worker sync ORPC lanes that means the
///   `spawn_blocking` pool thread, which can stall other sync RPCs for the
///   full delay. Use only for short, intentional chaos.
/// - An `async` point executes `Delay` with `tokio::time::sleep`, so only the
///   current request future is suspended. Prefer this for long delays.
///
/// The action JSON is the same (`{"type":"delay","duration_ms":...}`); which
/// sleep runs is determined solely by whether the call site used `sync` or
/// `async`. There is no separate "async delay" action.
///
/// A `return_error` closure constructs the surrounding function's natural
/// return value; RPC handlers must follow their sync or async wire contract.
///
/// Without the `fault-injection` feature every form expands to an empty
/// block, so instrumented call sites cost nothing in production builds.
#[cfg(feature = "fault-injection")]
#[macro_export]
macro_rules! fault_point {
    (
        sync,
        name: $name:literal,
        description: $description:literal,
        $(runtime: $runtime:expr,)?
        context: {$($key:literal => $value:expr),* $(,)?} $(,)?
    ) => {{
        let (__point, __decision) = $crate::__fault_point_prepare! {
            execution: $crate::FaultExecution::Sync,
            actions: [
                $crate::FaultActionKind::Record,
                $crate::FaultActionKind::Delay,
                $crate::FaultActionKind::Crash,
            ],
            name: $name,
            description: $description,
            $(runtime: $runtime,)?
            context: {$($key => $value),*},
        };
        if $crate::execute_sync_action(__decision).is_some() {
            panic!(
                "fault point {} produced ReturnError without a return_error handler",
                __point.name
            );
        }
    }};
    (
        sync,
        name: $name:literal,
        description: $description:literal,
        $(runtime: $runtime:expr,)?
        context: {$($key:literal => $value:expr),* $(,)?},
        return_error: $handler:expr $(,)?
    ) => {{
        let (_, __decision) = $crate::__fault_point_prepare! {
            execution: $crate::FaultExecution::Sync,
            actions: [
                $crate::FaultActionKind::Record,
                $crate::FaultActionKind::ReturnError,
                $crate::FaultActionKind::Delay,
                $crate::FaultActionKind::Crash,
            ],
            name: $name,
            description: $description,
            $(runtime: $runtime,)?
            context: {$($key => $value),*},
        };
        if let Some(__fault_error) = $crate::execute_sync_action(__decision) {
            return $crate::invoke_return_handler($handler, __fault_error);
        }
    }};
    (
        async,
        name: $name:literal,
        description: $description:literal,
        $(runtime: $runtime:expr,)?
        context: {$($key:literal => $value:expr),* $(,)?} $(,)?
    ) => {{
        let (__point, __decision) = $crate::__fault_point_prepare! {
            execution: $crate::FaultExecution::Async,
            actions: [
                $crate::FaultActionKind::Record,
                $crate::FaultActionKind::Delay,
                $crate::FaultActionKind::Crash,
            ],
            name: $name,
            description: $description,
            $(runtime: $runtime,)?
            context: {$($key => $value),*},
        };
        if $crate::execute_async_action(__decision).await.is_some() {
            panic!(
                "fault point {} produced ReturnError without a return_error handler",
                __point.name
            );
        }
    }};
    (
        async,
        name: $name:literal,
        description: $description:literal,
        $(runtime: $runtime:expr,)?
        context: {$($key:literal => $value:expr),* $(,)?},
        return_error: $handler:expr $(,)?
    ) => {{
        let (_, __decision) = $crate::__fault_point_prepare! {
            execution: $crate::FaultExecution::Async,
            actions: [
                $crate::FaultActionKind::Record,
                $crate::FaultActionKind::ReturnError,
                $crate::FaultActionKind::Delay,
                $crate::FaultActionKind::Crash,
            ],
            name: $name,
            description: $description,
            $(runtime: $runtime,)?
            context: {$($key => $value),*},
        };
        if let Some(__fault_error) = $crate::execute_async_action(__decision).await {
            return $crate::invoke_return_handler($handler, __fault_error).await;
        }
    }};
}

/// Shared no-op implementation for consumers that keep their own fault
/// feature disabled while Cargo enables the engine through another crate.
#[doc(hidden)]
#[macro_export]
macro_rules! __noop_fault_point {
    ($($tokens:tt)*) => {{}};
}

/// No-op form used when the `fault-injection` feature is disabled.
#[cfg(not(feature = "fault-injection"))]
pub use crate::__noop_fault_point as fault_point;

#[cfg(all(test, feature = "fault-injection"))]
mod tests {
    use super::*;

    fn runtime() -> FaultRuntime {
        FaultRuntime::isolated().unwrap()
    }

    fn sync_return(runtime: &FaultRuntime) -> Result<(), String> {
        fault_point! {
            sync,
            name: "client.macro.sync_return",
            description: "sync return macro test",
            runtime: runtime,
            context: {},
            return_error: |fault| Err(fault.message),
        }
        Ok(())
    }

    async fn async_return(runtime: &FaultRuntime) -> Result<(), String> {
        fault_point! {
            async,
            name: "client.macro.async_return",
            description: "async return macro test",
            runtime: runtime,
            context: {},
            return_error: |fault| async move { Err(fault.message) },
        }
        Ok(())
    }

    fn duplicate_first(runtime: &FaultRuntime) {
        fault_point! {
            sync,
            name: "client.macro.duplicate",
            description: "identical declarations are merged",
            runtime: runtime,
            context: {"rpc_code" => 1_i32},
        }
    }

    fn duplicate_second(runtime: &FaultRuntime) {
        fault_point! {
            sync,
            name: "client.macro.duplicate",
            description: "identical declarations are merged",
            runtime: runtime,
            context: {"rpc_code" => 2_i32},
        }
    }

    #[tokio::test]
    async fn fault_point_macro_executes_return_delay_and_merges_identical_points() {
        let runtime = runtime();
        for (id, point) in [
            ("sync-return", "client.macro.sync_return"),
            ("async-return", "client.macro.async_return"),
        ] {
            runtime
                .configure(
                    id,
                    ConfigureFaultRule {
                        point: point.to_string(),
                        action: FaultAction::ReturnError {
                            error: FaultErrorSpec {
                                message: id.to_string(),
                            },
                        },
                        ..Default::default()
                    },
                )
                .unwrap();
        }
        runtime
            .configure(
                "sync-delay",
                ConfigureFaultRule {
                    point: "client.macro.duplicate".to_string(),
                    action: FaultAction::Delay { duration_ms: 0 },
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(sync_return(&runtime).unwrap_err(), "sync-return");
        assert_eq!(async_return(&runtime).await.unwrap_err(), "async-return");
        duplicate_first(&runtime);
        duplicate_second(&runtime);

        let status = runtime.status();
        let duplicate_points = status
            .points
            .iter()
            .filter(|point| point.name == "client.macro.duplicate")
            .count();
        assert_eq!(duplicate_points, 1);
        let return_point = status
            .points
            .iter()
            .find(|point| point.name == "client.macro.sync_return")
            .unwrap();
        assert!(return_point.actions.contains(&FaultActionKind::ReturnError));
        assert!(return_point.actions.contains(&FaultActionKind::Delay));
        assert!(return_point.actions.contains(&FaultActionKind::Crash));
    }

    #[test]
    fn explicit_runtime_form_isolates_in_process_instances() {
        let first = runtime();
        let second = runtime();
        first
            .configure(
                "first-only",
                ConfigureFaultRule {
                    point: "client.macro.sync_return".to_string(),
                    action: FaultAction::ReturnError {
                        error: FaultErrorSpec {
                            message: "first".to_string(),
                        },
                    },
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(sync_return(&first).unwrap_err(), "first");
        assert!(sync_return(&second).is_ok());
    }
}
