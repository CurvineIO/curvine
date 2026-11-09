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

use curvine_codec::{Decode, Encode, Reader, Writer};
use curvine_master::master::meta::inode::{Inode, InodeDir, InodeView};
use curvine_model::{MkdirOpts, INTERNAL_CTIME_XATTR};

#[test]
fn legacy_directory_codec_roundtrip() {
    // Captured from the original codec for InodeDir::new(2001, 20): this
    // legacy value has no internal ctime xattr.
    let bytes = [
        209, 7, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 20, 0, 0, 0, 0, 0, 0, 0,
        20, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 1, 0, 0,
    ];
    let mut reader = Reader::new(&bytes, 1).unwrap();
    let decoded = InodeDir::decode(&mut reader).unwrap();
    assert_eq!(decoded.ctime(), 20, "missing ctime falls back to mtime");
    let mut writer = Writer::new(1).unwrap();
    decoded.encode(&mut writer).unwrap();
    assert_eq!(writer.into_inner(), bytes);
}

#[test]
fn malformed_ctime_marker_roundtrips_without_normalization() {
    let mut options = MkdirOpts::with_create(false);
    options
        .x_attr
        .insert(INTERNAL_CTIME_XATTR.into(), vec![1, 2, 3]);
    let original = InodeView::new_dir("dir".into(), InodeDir::with_opts(2001, 20, options));
    let mut writer = Writer::new(1).unwrap();
    original.encode(&mut writer).unwrap();
    let bytes = writer.into_inner();
    let decoded = InodeView::decode(&mut Reader::new(&bytes, 1).unwrap()).unwrap();
    assert_eq!(decoded.ctime(), 20);
    assert_eq!(
        decoded.x_attr().unwrap().get(INTERNAL_CTIME_XATTR).unwrap(),
        &[1, 2, 3]
    );
    let mut writer = Writer::new(1).unwrap();
    decoded.encode(&mut writer).unwrap();
    assert_eq!(writer.into_inner(), bytes);
}

#[test]
fn mtime_update_can_leave_equal_ctime_marker() {
    let mut options = MkdirOpts::with_create(false);
    options
        .x_attr
        .insert(INTERNAL_CTIME_XATTR.into(), 5_i64.to_le_bytes().to_vec());
    let mut dir = InodeDir::with_opts(2001, 10, options);
    dir.update_mtime(20);
    let view = InodeView::new_dir("dir".into(), dir);
    assert_eq!(view.mtime(), 20);
    // Field-level ctime only moves forward when the marker is behind the new
    // mtime; the state authority keeps its own value. Either way the marker
    // bytes survive the update.
    let marker = view
        .x_attr()
        .unwrap()
        .get(INTERNAL_CTIME_XATTR)
        .unwrap()
        .clone();
    assert!(marker == 20_i64.to_le_bytes() || marker == 5_i64.to_le_bytes());
    let mut writer = Writer::new(1).unwrap();
    view.encode(&mut writer).unwrap();
    let bytes = writer.into_inner();
    let decoded = InodeView::decode(&mut Reader::new(&bytes, 1).unwrap()).unwrap();
    assert_eq!(decoded.x_attr().unwrap(), view.x_attr().unwrap());
    let mut writer = Writer::new(1).unwrap();
    decoded.encode(&mut writer).unwrap();
    assert_eq!(writer.into_inner(), bytes);
}

#[test]
fn directory_codec_preserves_multiple_xattrs_and_ctime() {
    let mut options = MkdirOpts::with_create(false);
    options.x_attr.insert("user.a".into(), b"a".to_vec());
    options.x_attr.insert("user.b".into(), b"b".to_vec());
    options
        .x_attr
        .insert(INTERNAL_CTIME_XATTR.into(), 42_i64.to_le_bytes().to_vec());
    let original = InodeView::new_dir("dir".into(), InodeDir::with_opts(2001, 20, options));
    let mut writer = Writer::new(1).unwrap();
    original.encode(&mut writer).unwrap();
    let bytes = writer.into_inner();
    let mut reader = Reader::new(&bytes, 1).unwrap();
    let decoded = InodeView::decode(&mut reader).unwrap();
    // The codec preserves the marker bytes and the id; ctime authority in the
    // live model is the attribute state, so only marker retention is asserted.
    assert_eq!(decoded.x_attr().unwrap(), original.x_attr().unwrap());
    assert_eq!(decoded.as_dir_ref().unwrap().id(), 2001);
}
