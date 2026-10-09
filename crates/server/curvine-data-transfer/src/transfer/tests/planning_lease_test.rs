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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::dispatch::{await_with_lease_renewal, lease_renew_interval};

#[test]
fn planning_lease_renews_halfway_through_its_timeout() {
    assert_eq!(
        lease_renew_interval(Duration::from_secs(120)),
        Duration::from_secs(60)
    );
    assert_eq!(
        lease_renew_interval(Duration::ZERO),
        Duration::from_millis(1)
    );
}

#[tokio::test]
async fn planning_work_renews_until_it_finishes() {
    let renewals = Arc::new(AtomicUsize::new(0));
    let counter = renewals.clone();

    let result = await_with_lease_renewal(
        async {
            tokio::time::sleep(Duration::from_millis(12)).await;
            "done"
        },
        Duration::from_millis(2),
        move || {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(true)
        },
    )
    .await
    .unwrap();

    assert_eq!(result, Some("done"));
    assert!(renewals.load(Ordering::Relaxed) >= 1);
}

#[tokio::test]
async fn stale_planning_lease_stops_before_work_finishes() {
    let result = await_with_lease_renewal(
        std::future::pending::<()>(),
        Duration::from_millis(1),
        || Ok(false),
    )
    .await
    .unwrap();

    assert_eq!(result, None);
}
