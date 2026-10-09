#[cfg(feature = "transfer-store-mysql")]
mod mysql_tests {
    use super::super::mysql_store::job_where_mysql;
    use curvine_model::{TransferKind, TransferState};

    #[test]
    fn job_state_buckets_use_in_and_preserve_bind_order() {
        let (where_sql, values) = job_where_mysql(
            Some(TransferKind::Load),
            &[TransferState::Failed, TransferState::PartialSuccess],
            Some("alice"),
            Some("tenant-a"),
        );

        assert_eq!(
            where_sql,
            "where kind = ? and state in (?, ?) and submitter = ? and tenant = ?"
        );
        assert_eq!(
            values,
            vec![
                mysql::Value::Int(TransferKind::Load as i32 as i64),
                mysql::Value::Int(TransferState::Failed as i32 as i64),
                mysql::Value::Int(TransferState::PartialSuccess as i32 as i64),
                mysql::Value::Bytes(b"alice".to_vec()),
                mysql::Value::Bytes(b"tenant-a".to_vec()),
            ]
        );
    }

    #[test]
    fn job_filter_omits_empty_state_list() {
        let (where_sql, values) = job_where_mysql(None, &[], None, None);
        assert!(where_sql.is_empty());
        assert!(values.is_empty());
    }
}

#[cfg(feature = "transfer-store-postgres")]
mod postgres_tests {
    use super::super::postgres_store::transfer_filter_clause;
    use postgres::types::ToSql;

    #[test]
    fn job_state_buckets_use_in_and_preserve_bind_order() {
        let kind = Some(1);
        let states = [7, 9];
        let submitter = "alice".to_string();
        let tenant = "tenant-a".to_string();
        let mut clauses = Vec::new();
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();

        transfer_filter_clause(
            &kind,
            &states,
            Some(&submitter),
            Some(&tenant),
            &mut clauses,
            &mut params,
        );

        assert_eq!(
            clauses,
            [
                "kind = $1",
                "state in ($2, $3)",
                "submitter = $4",
                "tenant = $5"
            ]
        );
        assert_eq!(params.len(), 5);
    }

    #[test]
    fn job_filter_omits_empty_state_list() {
        let mut clauses = Vec::new();
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();

        transfer_filter_clause(&None, &[], None, None, &mut clauses, &mut params);

        assert!(clauses.is_empty());
        assert!(params.is_empty());
    }
}
