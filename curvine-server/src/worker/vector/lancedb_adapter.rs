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

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::connection::Connection;
use lancedb::query::{ExecutableQuery, QueryBase};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LanceWorkerVectorError {
    #[error("lancedb: {0}")]
    LanceDb(String),
    #[error("arrow: {0}")]
    Arrow(String),
}

#[derive(Debug)]
pub struct LanceDbVectorAdapter;

impl LanceDbVectorAdapter {
    pub async fn connect_local(
        dataset_uri: impl AsRef<str>,
    ) -> Result<Connection, LanceWorkerVectorError> {
        lancedb::connect(dataset_uri.as_ref())
            .execute()
            .await
            .map_err(|e| LanceWorkerVectorError::LanceDb(e.to_string()))
    }

    pub fn vector_schema(dim: i32) -> Result<Arc<Schema>, LanceWorkerVectorError> {
        Ok(Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
                false,
            ),
        ])))
    }

    pub async fn create_vector_table(
        conn: &Connection,
        table_name: &str,
        dim: i32,
        ids: &[i32],
        vectors: &[Vec<f32>],
    ) -> Result<(), LanceWorkerVectorError> {
        if ids.len() != vectors.len() {
            return Err(LanceWorkerVectorError::Arrow(
                "ids and vectors length mismatch".into(),
            ));
        }
        for row in vectors {
            if row.len() != dim as usize {
                return Err(LanceWorkerVectorError::Arrow(format!(
                    "vector dim mismatch: expected {}, got {}",
                    dim,
                    row.len()
                )));
            }
        }

        let schema = Self::vector_schema(dim)?;
        let id_arr = Int32Array::from_iter_values(ids.iter().copied());
        let vec_arr = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            vectors
                .iter()
                .map(|v| Some(v.iter().copied().map(Some).collect::<Vec<_>>())),
            dim,
        );
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_arr), Arc::new(vec_arr)])
            .map_err(|e| LanceWorkerVectorError::Arrow(e.to_string()))?;

        conn.create_table(table_name, vec![batch])
            .execute()
            .await
            .map_err(|e| LanceWorkerVectorError::LanceDb(e.to_string()))?;

        Ok(())
    }

    pub async fn open_table(
        conn: &Connection,
        table_name: &str,
    ) -> Result<lancedb::table::Table, LanceWorkerVectorError> {
        conn.open_table(table_name)
            .execute()
            .await
            .map_err(|e| LanceWorkerVectorError::LanceDb(e.to_string()))
    }

    pub async fn nearest_exact_smoke(
        table: &lancedb::table::Table,
        query: &[f32],
        limit: usize,
    ) -> Result<usize, LanceWorkerVectorError> {
        let stream = table
            .query()
            .nearest_to(query)
            .map_err(|e| LanceWorkerVectorError::LanceDb(e.to_string()))?
            .limit(limit)
            .execute()
            .await
            .map_err(|e| LanceWorkerVectorError::LanceDb(e.to_string()))?;

        let collected: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| LanceWorkerVectorError::LanceDb(e.to_string()))?;

        Ok(collected.iter().map(|b| b.num_rows()).sum())
    }
}
