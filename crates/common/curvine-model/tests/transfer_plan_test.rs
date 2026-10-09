use curvine_model::{
    agreed_plan_source_mtime, check_transfer_plan_size, classify_transfer_plan,
    group_resume_remaining, normalize_group_resume, parse_segment_group, plan_source_mtime,
    segment_group_json, SegmentGroupMismatch, SegmentGroupPlan, TransferPlanKind,
};

fn group() -> SegmentGroupPlan {
    SegmentGroupPlan {
        start_offset: 0,
        end_offset: 384,
        total_len: 512,
        first_block_index: 0,
        segment_bytes: 128,
        source_len: 512,
        source_mtime: 10,
        source_version: String::new(),
    }
}

#[test]
fn classifies_batch_segment_group_and_plain() {
    assert_eq!(
        classify_transfer_plan("{\"batch\":{\"batch\":[]}}"),
        TransferPlanKind::Batch
    );
    assert_eq!(
        classify_transfer_plan("{\"segment\":{\"offset\":0}}"),
        TransferPlanKind::Segment
    );
    assert_eq!(
        classify_transfer_plan("  {\"segment_group\":{\"start_offset\":0}}"),
        TransferPlanKind::SegmentGroup
    );
    assert_eq!(classify_transfer_plan(""), TransferPlanKind::Plain);
    assert_eq!(classify_transfer_plan("[]"), TransferPlanKind::Plain);
    assert_eq!(classify_transfer_plan("not json"), TransferPlanKind::Plain);
    assert_eq!(classify_transfer_plan("{}"), TransferPlanKind::Plain);
}

#[test]
fn range_manifest_round_trips_with_compact_payload() {
    let json = segment_group_json(&group()).unwrap();
    assert!(json.len() < 1024);
    check_transfer_plan_size(&json).unwrap();
    assert_eq!(
        classify_transfer_plan(&json),
        TransferPlanKind::SegmentGroup
    );
    assert_eq!(parse_segment_group(&json).unwrap(), group());
}

#[test]
fn group_len_resume_floor_and_iter_cover_segments() {
    let plan = group();
    assert_eq!(plan.group_len(), 384);
    assert_eq!(plan.resume_floor(-5), 0);
    assert_eq!(plan.resume_floor(0), 0);
    assert_eq!(plan.resume_floor(1), 0);
    assert_eq!(plan.resume_floor(127), 0);
    assert_eq!(plan.resume_floor(128), 128);
    assert_eq!(plan.resume_floor(200), 128);
    assert_eq!(plan.resume_floor(383), 256);
    assert_eq!(plan.resume_floor(384), 384);
    assert_eq!(plan.resume_floor(10_000), 384);
    let all = plan.iter_from(0);
    assert_eq!(all, vec![(0, 128, 0), (128, 128, 1), (256, 128, 2)]);
    assert_eq!(plan.iter_from(200), vec![(128, 128, 1), (256, 128, 2)]);
    assert_eq!(plan.iter_from(384), Vec::new());
}

#[test]
fn partial_tail_group_iterates_short_last_segment() {
    let plan = SegmentGroupPlan {
        start_offset: 256,
        end_offset: 512,
        total_len: 512,
        first_block_index: 2,
        segment_bytes: 128,
        source_len: 512,
        source_mtime: 0,
        source_version: String::new(),
    };
    assert_eq!(plan.group_len(), 256);
    assert_eq!(plan.iter_from(0), vec![(256, 128, 2), (384, 128, 3)]);
    // File-tail short segment must end at total_len to stay block-aligned.
    let short = SegmentGroupPlan {
        end_offset: 500,
        total_len: 500,
        source_len: 500,
        ..plan.clone()
    };
    assert!(short.validate().is_ok());
    assert_eq!(short.iter_from(0), vec![(256, 128, 2), (384, 116, 3)]);
    // Reported beyond group_len means the tail is complete.
    assert_eq!(short.resume_floor(256), 244);
    // Misaligned mid-file cut is invalid and must never be planned.
    let misaligned = SegmentGroupPlan {
        end_offset: 500,
        ..plan.clone()
    };
    assert!(misaligned.validate().is_err());
}

#[test]
fn invalid_manifest_is_rejected() {
    let bad = SegmentGroupPlan {
        start_offset: 100,
        end_offset: 100,
        ..group()
    };
    assert!(bad.validate().is_err());
    assert!(segment_group_json(&bad).is_err());
    let over = SegmentGroupPlan {
        end_offset: 600,
        ..group()
    };
    assert!(over.validate().is_err());
    assert!(parse_segment_group("{\"segment_group\":{}}").is_err());
    assert!(parse_segment_group("{\"segment\":{\"offset\":0}}").is_err());
}

#[test]
fn oversized_payload_is_rejected() {
    let big = "x".repeat(9000);
    assert!(check_transfer_plan_size(&big).is_err());
}

#[test]
fn normalize_group_resume_floors_and_clamps() {
    let json = segment_group_json(&group()).unwrap();
    assert_eq!(normalize_group_resume(&json, -10), 0);
    assert_eq!(normalize_group_resume(&json, 0), 0);
    assert_eq!(normalize_group_resume(&json, 200), 128);
    assert_eq!(normalize_group_resume(&json, 384), 384);
    assert_eq!(normalize_group_resume(&json, 10_000), 384);
    assert_eq!(normalize_group_resume("{\"segment_group\":{}}", 200), 0);
    assert_eq!(normalize_group_resume("not json", 200), 0);
    assert_eq!(
        normalize_group_resume("{\"segment\":{\"offset\":0}}", 200),
        0
    );
}

#[test]
fn group_resume_remaining_reports_remaining_bytes() {
    let json = segment_group_json(&group()).unwrap();
    assert_eq!(group_resume_remaining(&json, 0), (0, 384));
    assert_eq!(group_resume_remaining(&json, 200), (128, 256));
    assert_eq!(group_resume_remaining(&json, 384), (384, 0));
    assert_eq!(group_resume_remaining("bad", 200), (0, 0));
}

#[test]
fn stat_identity_reidentifies_an_unchanged_source() {
    // The default manifest carries a HEAD (len, mtime) pair and no version:
    // this is the only identity an OSS-HDFS adapter can supply, so it must be
    // enough to run and to resume.
    let plan = group();
    assert!(plan.has_stat_identity());
    assert!(plan.has_identity());
    assert_eq!(plan.verify_observation(512, 10, ""), Ok(()));
}

#[test]
fn stat_identity_rejects_every_observable_change() {
    let plan = group();
    assert_eq!(
        plan.verify_observation(511, 10, ""),
        Err(SegmentGroupMismatch::LengthChanged)
    );
    assert_eq!(
        plan.verify_observation(512, 11, ""),
        Err(SegmentGroupMismatch::MtimeChanged)
    );
}

#[test]
fn version_identity_outranks_mtime_and_never_degrades() {
    let mut plan = group();
    plan.source_version = "etag-1".to_string();
    // The pinned version still decides; a diagnostic mtime match is not enough
    // when the backend swaps the object under the same mtime.
    assert_eq!(
        plan.verify_observation(512, 10, "etag-2"),
        Err(SegmentGroupMismatch::VersionChanged)
    );
    assert_eq!(
        plan.verify_observation(512, 10, ""),
        Err(SegmentGroupMismatch::VersionUnavailable)
    );
    assert_eq!(plan.verify_observation(512, 10, "etag-1"), Ok(()));
}

#[test]
fn manifest_without_identity_is_refused() {
    let mut plan = group();
    plan.source_len = 0;
    plan.source_mtime = 0;
    assert!(!plan.has_stat_identity());
    assert!(!plan.has_identity());
    assert_eq!(
        plan.verify_observation(512, 10, ""),
        Err(SegmentGroupMismatch::NoIdentity)
    );
}

#[test]
fn version_only_manifest_skips_the_mtime_gate() {
    // Legacy manifests pin a version but no stat pair; they must keep working.
    let mut plan = group();
    plan.source_len = 512;
    plan.source_mtime = 0;
    plan.source_version = "etag-1".to_string();
    assert!(plan.has_identity());
    assert_eq!(plan.verify_observation(512, 999, "etag-1"), Ok(()));
}

#[test]
fn segment_manifest_carries_the_plan_time_source_mtime() {
    // The scheduler stamps this value as the target's UFS identity at
    // convergence, so a segment manifest must persist it and old manifests
    // (written before the field existed) must still parse.
    let plan = serde_json::json!({
        "segment": {
            "offset": 0,
            "length": 128,
            "total_len": 512,
            "block_index": 0,
            "source_mtime": 1_789_595_866_000i64
        }
    })
    .to_string();
    assert_eq!(plan_source_mtime(&plan), 1_789_595_866_000);

    let legacy = serde_json::json!({
        "segment": { "offset": 0, "length": 128, "total_len": 512, "block_index": 0 }
    })
    .to_string();
    assert_eq!(plan_source_mtime(&legacy), 0);
}

#[test]
fn plan_source_mtime_reads_group_and_rejects_other_plans() {
    let mut grouped = group();
    grouped.source_mtime = 42;
    assert_eq!(
        plan_source_mtime(&segment_group_json(&grouped).unwrap()),
        42
    );
    assert_eq!(plan_source_mtime("{\"batch\":{\"batch\":[]}}"), 0);
    assert_eq!(plan_source_mtime(""), 0);
    assert_eq!(plan_source_mtime("not json"), 0);
}

#[test]
fn convergence_requires_every_segment_to_agree_on_the_source_mtime() {
    let mtime = 1_789_595_866_000i64;
    let seg = |mtime: i64| {
        serde_json::json!({
            "segment": {
                "offset": 0, "length": 128, "total_len": 512, "block_index": 0,
                "source_mtime": mtime
            }
        })
        .to_string()
    };

    let a = seg(mtime);
    let b = seg(mtime);
    assert_eq!(
        agreed_plan_source_mtime([a.as_str(), b.as_str()]),
        Some(mtime)
    );

    // A source that moved mid-copy leaves the plans disagreeing: no single
    // mtime describes the cached bytes, so the target stays cache-invalid.
    let moved = seg(mtime + 1);
    assert_eq!(agreed_plan_source_mtime([a.as_str(), moved.as_str()]), None);
    // A plan without the observation must not be invented either.
    let unknown = seg(0);
    assert_eq!(
        agreed_plan_source_mtime([a.as_str(), unknown.as_str()]),
        None
    );
    assert_eq!(agreed_plan_source_mtime([unknown.as_str()]), None);
    assert_eq!(agreed_plan_source_mtime(Vec::<&str>::new()), None);
}
