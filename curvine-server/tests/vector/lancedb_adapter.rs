use curvine_server::worker::vector::LanceDbVectorAdapter;
use tempfile::tempdir;

#[tokio::test]
async fn local_dataset_create_write_exact_query_roundtrip() {
    let dir = tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let conn = LanceDbVectorAdapter::connect_local(uri).await.unwrap();

    let dim = 4_i32;
    let ids = vec![1, 2, 3];
    let vectors = vec![
        vec![1.0_f32, 0.0, 0.0, 0.0],
        vec![0.0_f32, 1.0, 0.0, 0.0],
        vec![1.0_f32, 1.0, 0.0, 0.0],
    ];

    LanceDbVectorAdapter::create_vector_table(&conn, "t_vectors", dim, &ids, &vectors)
        .await
        .unwrap();

    let table = LanceDbVectorAdapter::open_table(&conn, "t_vectors")
        .await
        .unwrap();
    let query = vec![1.0_f32, 0.0, 0.0, 0.0];
    let hits = LanceDbVectorAdapter::nearest_exact_smoke(&table, &query, 5)
        .await
        .unwrap();
    assert!(hits > 0);
}
