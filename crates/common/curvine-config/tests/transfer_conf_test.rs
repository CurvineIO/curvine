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

use curvine_config::{TransferConf, TransferStoreType};

#[test]
fn postgres_store_urls_select_the_postgres_backend() {
    for store_url in [
        "postgres://transfer:secret@db.example:5432/curvine_transfer",
        "postgresql://transfer:secret@db.example:5432/curvine_transfer",
    ] {
        let mut conf = TransferConf {
            enabled: true,
            store_url: store_url.to_string(),
            ..Default::default()
        };

        conf.init().unwrap();

        assert_eq!(conf.effective_store_type(), TransferStoreType::Postgres);
    }
}
