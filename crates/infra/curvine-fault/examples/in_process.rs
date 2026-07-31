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

//! In-process fault injection: declare a point, install a rule, observe it.
//!
//! ```bash
//! cargo run -p curvine-fault --example in_process --features fault-injection
//! ```

use curvine_fault::{fault_point, FaultRuleBuilder, FaultRuntime};

fn read_block(block_id: i64) -> Result<Vec<u8>, String> {
    fault_point! {
        sync,
        name: "storage.block.before_read",
        description: "Before a block read is served",
        context: { "block_id" => block_id },
        return_error: |fault| Err(fault.message),
    }
    Ok(vec![0_u8; 16])
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = FaultRuntime::process();

    // Fail only block 7, only once.
    let rule = FaultRuleBuilder::named("storage.block.before_read")
        .matches("block_id", 7_i64)?
        .times(1)?
        .return_error("injected read failure")?;
    runtime.configure("fail-block-7-once", rule)?;

    assert!(read_block(1).is_ok());
    assert_eq!(read_block(7).unwrap_err(), "injected read failure");
    assert!(read_block(7).is_ok(), "max_hits=1 exhausts the rule");

    let status = runtime.status();
    let rule = &status.rules[0];
    println!(
        "rule {}: evaluations={} matches={} executions={}",
        rule.id, rule.evaluations, rule.matches, rule.executions
    );
    Ok(())
}
