# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python3
"""
Comprehensive S3 Test Suite using boto3
Tests all S3 operations, streaming, range requests, error handling, and performance
"""

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import sys
import os
import time
import hashlib
import tempfile
import threading
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional
import json


class S3TestConfig:
    """Configuration for S3 tests"""
    
    def __init__(self):
        self.endpoint_url = 'http://localhost:9900'
        # Read the credentials from the environment; see the README for how to
        # create one with `curvine-s3-gateway credential generate`.
        self.access_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
        self.secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
        self.region = 'us-east-1'
        self.test_bucket = f'python-test-{int(time.time())}'
        self.temp_dir = tempfile.mkdtemp(prefix='curvine-s3-test-')


class S3TestClient:
    """S3 test client wrapper"""
    
    def __init__(self, config: S3TestConfig):
        self.config = config
        self.client = boto3.client(
            's3',
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            region_name=config.region,
            config=Config(signature_version='s3v4', retries={'max_attempts': 1})
        )
    
    def create_bucket(self) -> bool:
        """Create test bucket"""
        try:
            self.client.create_bucket(Bucket=self.config.test_bucket)
            return True
        except ClientError as e:
            print(f"Failed to create bucket: {e}")
            return False
    
    def delete_bucket(self) -> bool:
        """Delete test bucket"""
        try:
            self.client.delete_bucket(Bucket=self.config.test_bucket)
            return True
        except ClientError as e:
            print(f"Failed to delete bucket: {e}")
            return False
    
    def bucket_exists(self) -> bool:
        """Check if bucket exists"""
        try:
            self.client.head_bucket(Bucket=self.config.test_bucket)
            return True
        except ClientError:
            return False


class TestDataGenerator:
    """Generate test data of various types and sizes"""
    
    @staticmethod
    def create_text_data(size: int) -> bytes:
        """Create text data of specified size"""
        pattern = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        data = (pattern * (size // len(pattern) + 1))[:size]
        return data.encode('utf-8')
    
    @staticmethod
    def create_binary_data(size: int) -> bytes:
        """Create binary data of specified size"""
        return os.urandom(size)
    
    @staticmethod
    def create_zero_data(size: int) -> bytes:
        """Create zero-filled data of specified size"""
        return b'\x00' * size
    
    @staticmethod
    def create_empty_data() -> bytes:
        """Create empty data"""
        return b''
    
    @staticmethod
    def create_special_chars_data() -> bytes:
        """Create data with special characters"""
        return "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?`~".encode('utf-8')


class S3BasicOperationsTest:
    """Test basic S3 operations"""
    
    def __init__(self, client: S3TestClient):
        self.client = client
        self.test_results = []
    
    def test_bucket_operations(self) -> bool:
        """Test bucket creation, head, and deletion"""
        print("Testing bucket operations...")
        
        # Test create bucket
        success = self.client.create_bucket()
        self._record_test("Create Bucket", success)
        if not success:
            return False
        
        # Test head bucket
        success = self.client.bucket_exists()
        self._record_test("Head Bucket", success)
        
        return success
    
    def test_put_object(self, key: str, data: bytes, metadata: Optional[Dict] = None) -> bool:
        """Test putting an object"""
        try:
            kwargs = {
                'Bucket': self.client.config.test_bucket,
                'Key': key,
                'Body': data
            }
            if metadata:
                kwargs['Metadata'] = metadata
            
            self.client.client.put_object(**kwargs)
            self._record_test(f"Put Object ({key})", True)
            return True
        except ClientError as e:
            print(f"Failed to put object {key}: {e}")
            self._record_test(f"Put Object ({key})", False)
            return False
    
    def test_list_multipart_uploads(self) -> bool:
        import time
        from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

        s3, bucket = self.client.client, self.client.config.test_bucket
        other = f'{bucket[:45]}-mpu-other'
        prefix = 'mpu +中/'
        key = prefix + 'a'
        child = prefix + 'dir/nested'
        tail = prefix + 'z'
        pending = []

        def start(b, k):
            params = dict(Bucket=b, Key=k)
            params['UploadId'] = s3.create_multipart_upload(**params)['UploadId']
            pending.append(params)
            return params

        def listing(**kwargs):
            return s3.list_multipart_uploads(Bucket=bucket, Prefix=prefix, **kwargs)

        def uploads(page):
            return [(u['Key'], u['UploadId']) for u in page.get('Uploads', [])]

        def error(b, **kwargs):
            try:
                s3.list_multipart_uploads(Bucket=b, **kwargs)
            except ClientError as e:
                return e.response['Error']['Code']

        s3.create_bucket(Bucket=other)
        try:
            empty = listing()
            first, second = sorted([start(bucket, key), start(bucket, key)], key=lambda u: u['UploadId'])
            nested, last = start(bucket, child), start(bucket, tail)
            start(other, key)
            p1 = listing(Delimiter='/', MaxUploads=1)
            p2 = listing(Delimiter='/', MaxUploads=1, KeyMarker=p1['NextKeyMarker'],
                         UploadIdMarker=p1['NextUploadIdMarker'])
            p3 = listing(Delimiter='/', MaxUploads=1, KeyMarker=p2['NextKeyMarker'],
                         UploadIdMarker=p2['NextUploadIdMarker'])
            p4 = listing(Delimiter='/', MaxUploads=1, KeyMarker=p3['NextKeyMarker'])
            capped = listing(MaxUploads=5000)
            encoded = listing(Delimiter='/', EncodingType='url', MaxUploads=1)
            time.sleep(0.025)
            part = s3.upload_part(**first, PartNumber=1, Body=b'list-multipart-uploads')
            initiated = listing(MaxUploads=1)['Uploads'][0]['Initiated']
            s3.complete_multipart_upload(**first, MultipartUpload={
                'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]})
            pending.remove(first)
            s3.abort_multipart_upload(**nested)
            pending.remove(nested)
            finished = listing()

            def rewrite_request(extra_path='', drop_uploads=False):
                def hook(request, **_):
                    u = urlsplit(request.url)
                    pairs = [(k, v) for k, v in parse_qsl(u.query, keep_blank_values=True)
                             if not (drop_uploads and k == 'uploads')]
                    if drop_uploads:
                        pairs.insert(0, ('x-id', 'ListMultipartUploads'))
                    q = '&'.join(k if v == '' and k == 'uploads' else urlencode([(k, v)]) for k, v in pairs)
                    request.url = urlunsplit((u.scheme, u.netloc, u.path + extra_path, q, ''))
                s3.meta.events.register('before-sign.s3.ListMultipartUploads', hook)
                try:
                    return error(bucket)
                finally:
                    s3.meta.events.unregister('before-sign.s3.ListMultipartUploads', hook)

            route_errors = [
                rewrite_request(extra_path='///'),
                rewrite_request(drop_uploads=True),
            ]
            success = (
                not uploads(empty) and not empty['IsTruncated']
                and all(p['IsTruncated'] for p in (p1, p2, p3)) and not p4['IsTruncated']
                and uploads(p1) == [(key, first['UploadId'])]
                and uploads(p2) == [(key, second['UploadId'])]
                and uploads(p3) == [] and p3['CommonPrefixes'] == [{'Prefix': prefix + 'dir/'}]
                and not p3.get('NextUploadIdMarker') and 'NextKeyMarker' not in p4
                and uploads(p4) == [(tail, last['UploadId'])]
                and capped['MaxUploads'] == 1000 and len(uploads(capped)) == 4
                and encoded['EncodingType'] == 'url'
                and uploads(encoded) == [(quote(key), first['UploadId'])]
                and encoded['NextKeyMarker'] == quote(key) and encoded['Prefix'] == quote(prefix)
                and initiated == p1['Uploads'][0]['Initiated']
                and uploads(finished) == [(key, second['UploadId']), (tail, last['UploadId'])]
                and error(f'{other}-missing') == 'NoSuchBucket'
                and error(bucket, MaxUploads=-1) == 'InvalidArgument'
                and error(bucket, EncodingType='invalid') == 'InvalidArgument'
                and route_errors == ['InvalidArgument', 'InvalidArgument']
            )
        finally:
            for params in pending:
                s3.abort_multipart_upload(**params)
            s3.delete_object(Bucket=bucket, Key=key)
            s3.delete_bucket(Bucket=other)
        self._record_test('ListMultipartUploads (pagination, encoding, complete/abort)', success)
        return success

    def test_overwrite_is_atomic(self, key: str) -> bool:
        """A successful overwrite must replace the object in one visible step.

        The failure half of this property is covered by
        `test_local_write_part_stream_removes_a_partial_part` and the staging
        unit tests, not here: a short body is rejected by hyper with 400 before
        the handler runs, so an early socket close cannot reach the staging
        code and would make this assertion pass trivially. What is testable from
        outside is that a normal overwrite publishes the new body whole.
        """
        v1 = b"first version of the object"
        v2 = b"second version, longer than the first"

        if not self.test_put_object(key, v1):
            return False
        ok, body = self.test_get_object(key)
        if not ok or body != v1:
            print("  FAIL: first version unreadable")
            return False

        if not self.test_put_object(key, v2):
            return False
        ok, body = self.test_get_object(key)
        if not ok or body != v2:
            print(f"  FAIL: overwrite did not publish the second version ({body!r})")
            return False

        # The ETag must follow the content, which is what makes the atomic
        # publish observable to a client.
        import subprocess

        head = subprocess.run(
            [
                "aws",
                "--endpoint-url",
                self.client.config.endpoint_url,
                "s3api",
                "head-object",
                "--bucket",
                self.client.config.test_bucket,
                "--key",
                key,
                "--query",
                "ETag",
                "--output",
                "text",
            ],
            capture_output=True,
            text=True,
        )
        expected = '"' + hashlib.md5(v2).hexdigest() + '"'
        if head.stdout.strip() != expected:
            print(f"  FAIL: ETag {head.stdout.strip()} != content md5 {expected}")
            return False
        return True

    def test_get_object(self, key: str) -> Tuple[bool, Optional[bytes]]:
        """Test getting an object"""
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key
            )
            data = response['Body'].read()
            self._record_test(f"Get Object ({key})", True)
            return True, data
        except ClientError as e:
            print(f"Failed to get object {key}: {e}")
            self._record_test(f"Get Object ({key})", False)
            return False, None
    
    def test_head_object(self, key: str) -> bool:
        """Test head object operation"""
        try:
            response = self.client.client.head_object(
                Bucket=self.client.config.test_bucket,
                Key=key
            )
            self._record_test(f"Head Object ({key})", True)
            return True
        except ClientError as e:
            print(f"Failed to head object {key}: {e}")
            self._record_test(f"Head Object ({key})", False)
            return False

    def test_head_response_overrides(self, key: str) -> bool:
        """#2185: HEAD applies all six response-* overrides and rejects invalid values with 400."""
        import datetime
        s3, bucket = self.client.client, self.client.config.test_bucket
        expected = {
            'cache-control': 'no-cache',
            'content-disposition': 'attachment; filename="x.txt"',
            'content-encoding': 'identity',
            'content-language': 'mi, en',
            'content-type': 'audio/ogg',
            'expires': 'Wed, 02 Jan 2030 03:04:05 GMT',
        }
        try:
            headers = s3.head_object(
                Bucket=bucket,
                Key=key,
                ResponseCacheControl='no-cache',
                ResponseContentDisposition='attachment; filename="x.txt"',
                ResponseContentEncoding='identity',
                ResponseContentLanguage='mi, en',
                ResponseContentType='audio/ogg',
                ResponseExpires=datetime.datetime(2030, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
            )['ResponseMetadata']['HTTPHeaders']
            valid_ok = all(headers.get(k) == v for k, v in expected.items())
        except ClientError:
            valid_ok = False
        try:
            s3.head_object(Bucket=bucket, Key=key, ResponseCacheControl='no-cache\r\nx-injected: 1')
            invalid_ok = False
        except ClientError as e:
            invalid_ok = e.response['ResponseMetadata']['HTTPStatusCode'] == 400
        success = valid_ok and invalid_ok
        self._record_test(f"Head Object response-* overrides ({key})", success)
        return success
    
    def test_upload_part_copy(self) -> bool:
        """#2187: UploadPartCopy stages ranges of an existing object as parts."""
        s3, bucket = self.client.client, self.client.config.test_bucket
        data = os.urandom(6 * 1024 * 1024)
        s3.put_object(Bucket=bucket, Key='upc-src', Body=data)
        source = {'Bucket': bucket, 'Key': 'upc-src'}
        s3.put_object(Bucket=bucket, Key='upc-dst', Body=b'keep-until-complete')
        upload = s3.create_multipart_upload(Bucket=bucket, Key='upc-dst')['UploadId']
        cut = 5 * 1024 * 1024
        parts = [{'PartNumber': n, 'ETag': s3.upload_part_copy(
            Bucket=bucket, Key='upc-dst', UploadId=upload, PartNumber=n, CopySource=source,
            CopySourceRange=r)['CopyPartResult']['ETag']}
            for n, r in ((1, f'bytes=0-{cut - 1}'), (2, f'bytes={cut}-{len(data) - 1}'))]
        import re
        etag_shaped = all(re.fullmatch(r'"[0-9a-f]{32}"', p['ETag']) for p in parts)

        def err_code(**kwargs):
            try:
                s3.upload_part_copy(**kwargs)
            except ClientError as e:
                return e.response['Error']['Code']

        params = dict(Bucket=bucket, Key='upc-dst', UploadId=upload, PartNumber=3,
                      CopySource=source)
        errors = [
            err_code(**dict(params, CopySourceRange=f'bytes=0-{len(data)}')),
            err_code(**dict(params, UploadId='../escape')),
            err_code(**dict(params, CopySource={'Bucket': 'no-such-bucket-upc', 'Key': 'k'})),
            err_code(**dict(params, Key='wrong-session-target')),
            err_code(**dict(params, Bucket='no-such-bucket-upc')),
            err_code(**dict(params, CopySource={'Bucket': bucket, 'Key': 'missing-source'})),
            err_code(**dict(params, CopySourceIfMatch='"not-the-source-etag"')),
        ]
        # Strip query params before signing so the router itself is exercised.
        from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
        for missing in ('partNumber', 'uploadId'):
            def strip(request, m=missing, **_):
                u = urlsplit(request.url)
                q = urlencode([(k, v) for k, v in parse_qsl(u.query) if k != m])
                request.url = urlunsplit((u.scheme, u.netloc, u.path, q, ''))
            s3.meta.events.register('before-sign.s3.UploadPartCopy', strip)
            try:
                errors.append(err_code(**params))
            finally:
                s3.meta.events.unregister('before-sign.s3.UploadPartCopy', strip)
        untouched = s3.get_object(Bucket=bucket, Key='upc-dst')['Body'].read() == b'keep-until-complete'
        s3.complete_multipart_upload(Bucket=bucket, Key='upc-dst', UploadId=upload,
                                     MultipartUpload={'Parts': parts})
        copied = s3.get_object(Bucket=bucket, Key='upc-dst')['Body'].read()
        success = (copied == data and etag_shaped and untouched
                   and errors == ['InvalidRange', 'NoSuchUpload', 'NoSuchBucket',
                                  'NoSuchUpload', 'NoSuchBucket', 'NoSuchKey',
                                  'PreconditionFailed', 'InvalidArgument', 'InvalidArgument'])
        self._record_test(f"UploadPartCopy (etag_ok={etag_shaped}, errors={errors})", success)
        return success

    def test_presigned_get(self) -> bool:
        """#2093: presigned GET/HEAD work; tampered or method-swapped URLs fail."""
        import urllib.error
        import urllib.request
        s3, bucket, key = self.client.client, self.client.config.test_bucket, 'presigned.txt'
        s3.put_object(Bucket=bucket, Key=key, Body=b'presigned body')
        params = {'Bucket': bucket, 'Key': key}
        get = s3.generate_presigned_url('get_object', Params=params, ExpiresIn=60)
        head = s3.generate_presigned_url('head_object', Params=params, ExpiresIn=60)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        def fetch(url, method='GET'):
            try:
                with opener.open(urllib.request.Request(url, method=method), timeout=5) as r:
                    return r.status, r.headers.get('Content-Length'), r.read()
            except urllib.error.HTTPError as error:
                return error.code, None, b''

        # The URLs are bearer credentials: never print them.
        success = (fetch(get) == (200, '14', b'presigned body')
                   and fetch(head, 'HEAD') == (200, '14', b'')
                   and fetch(get.replace(key, 'other.txt'))[0] == 403
                   and fetch(get, 'HEAD')[0] == 403
                   and fetch(head)[0] == 403)
        s3.delete_object(Bucket=bucket, Key=key)
        self._record_test("Presigned GET/HEAD (valid works, tampered or swapped rejected)", success)
        return success

    def test_delete_object(self, key: str) -> bool:
        """Test deleting an object"""
        try:
            self.client.client.delete_object(
                Bucket=self.client.config.test_bucket,
                Key=key
            )
            self._record_test(f"Delete Object ({key})", True)
            return True
        except ClientError as e:
            print(f"Failed to delete object {key}: {e}")
            self._record_test(f"Delete Object ({key})", False)
            return False
    
    def test_list_objects(self, prefix: Optional[str] = None) -> Tuple[bool, List[str]]:
        """Test listing objects"""
        try:
            kwargs = {'Bucket': self.client.config.test_bucket}
            if prefix:
                kwargs['Prefix'] = prefix
            
            response = self.client.client.list_objects_v2(**kwargs)
            objects = [obj['Key'] for obj in response.get('Contents', [])]
            
            test_name = f"List Objects" + (f" (prefix: {prefix})" if prefix else "")
            self._record_test(test_name, True)
            return True, objects
        except ClientError as e:
            print(f"Failed to list objects: {e}")
            self._record_test("List Objects", False)
            return False, []
    
    def test_list_parts(self) -> bool:
        """#2223: ListParts returns staged parts in order, pages, and checks session/bucket."""
        s3, bucket = self.client.client, self.client.config.test_bucket
        key = 'lp dir/a +b中.bin'
        upload = s3.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
        empty = s3.list_parts(Bucket=bucket, Key=key, UploadId=upload)
        p1 = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload, PartNumber=2, Body=b'b' * 11)['ETag']
        p2 = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload, PartNumber=1, Body=b'a' * 7)['ETag']
        p3 = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload, PartNumber=3, Body=b'c' * 13)['ETag']

        page1 = s3.list_parts(Bucket=bucket, Key=key, UploadId=upload, MaxParts=2)
        page2 = s3.list_parts(Bucket=bucket, Key=key, UploadId=upload, MaxParts=2,
                              PartNumberMarker=page1['NextPartNumberMarker'])
        capped = s3.list_parts(Bucket=bucket, Key=key, UploadId=upload, MaxParts=5000)

        def err_code(**kw):
            try:
                s3.list_parts(**kw)
            except ClientError as e:
                return e.response['Error']['Code']

        pre_errors = [
            err_code(Bucket='no-such-bucket-lp', Key=key, UploadId=upload),
            err_code(Bucket=bucket, Key='wrong-key', UploadId=upload),
            err_code(Bucket=bucket, Key=key, UploadId='../escape'),
            err_code(Bucket=bucket, Key=key, UploadId=upload, MaxParts=-1),
        ]
        from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
        def strip_upload_id(request, **_):
            u = urlsplit(request.url)
            q = urlencode([('x-id', 'ListParts')] + [(k, v) for k, v in parse_qsl(u.query) if k != 'uploadId'])
            request.url = urlunsplit((u.scheme, u.netloc, u.path, q, ''))
        s3.meta.events.register('before-sign.s3.ListParts', strip_upload_id)
        try:
            pre_errors.append(err_code(Bucket=bucket, Key=key, UploadId=upload))
        finally:
            s3.meta.events.unregister('before-sign.s3.ListParts', strip_upload_id)
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
        after_abort = err_code(Bucket=bucket, Key=key, UploadId=upload)

        got1 = [(p['PartNumber'], p['Size'], p['ETag']) for p in page1.get('Parts', [])]
        got2 = [(p['PartNumber'], p['Size'], p['ETag']) for p in page2.get('Parts', [])]
        no_cursor_when_complete = all('NextPartNumberMarker' not in r for r in (empty, page2, capped))
        success = (
            empty.get('Parts', []) == [] and not empty['IsTruncated']
            and page1['Key'] == key and page1['Owner']['ID'] == 'curvine-owner'
            and page1['IsTruncated'] and not page2['IsTruncated']
            and capped['MaxParts'] == 1000 and no_cursor_when_complete
            and got1 == [(1, 7, p2), (2, 11, p1)]
            and got2 == [(3, 13, p3)]
            and pre_errors == ['NoSuchBucket', 'NoSuchUpload', 'NoSuchUpload',
                               'InvalidArgument', 'InvalidArgument']
            and after_abort == 'NoSuchUpload'
        )
        self._record_test(f"ListParts (pages={len(got1)}+{len(got2)}, after_abort={after_abort})", success)
        return success

    def test_list_objects_v2_pagination(self) -> bool:
        """#2091: nested keys and CommonPrefixes page together without gaps."""
        s3, bucket = self.client.client, self.client.config.test_bucket
        keys = ['ls/' + k for k in ('a-', 'a/deep/1', 'a/deep/2', 'a/file', 'a0', '中文 +%.txt')]
        success = False
        try:
            for key in keys:
                s3.put_object(Bucket=bucket, Key=key, Body=b'x')
            for prefix, delimiter, size in (('ls/', '', 1), ('ls/', '/', 1),
                                            ('ls/a/d', '', 3), ('ls/', 'deep', 2)):
                expected = set()
                for key in filter(lambda k: k.startswith(prefix), keys):
                    at = key.find(delimiter, len(prefix)) if delimiter else -1
                    expected.add(key[:at + len(delimiter)] if at >= 0 else key)
                actual = []
                for page in s3.get_paginator('list_objects_v2').paginate(
                        Bucket=bucket, Prefix=prefix, Delimiter=delimiter,
                        PaginationConfig={'PageSize': size}):
                    actual += [i['Key'] for i in page.get('Contents', [])]
                    actual += [i['Prefix'] for i in page.get('CommonPrefixes', [])]
                    assert len(actual) <= len(expected), 'pagination did not terminate'
                assert sorted(actual) == sorted(expected), f'{prefix!r} {delimiter!r}: {actual}'
            zero = s3.list_objects_v2(Bucket=bucket, Prefix='ls/', MaxKeys=0)
            assert zero['KeyCount'] == 0 and not zero['IsTruncated'], 'MaxKeys=0'
            success = True
        except (ClientError, AssertionError) as error:
            print(f'ListObjectsV2 regression failed: {error}')
        finally:
            for key in keys:
                try:
                    s3.delete_object(Bucket=bucket, Key=key)
                except ClientError:
                    pass
            self._record_test('ListObjectsV2 nested keys and CommonPrefixes (#2091)', success)
        return success

    def test_directory_markers(self) -> bool:
        """Directory markers are exact keys and obey delimiter grouping (#2211)."""
        s3 = self.client.client
        bucket = f'{self.client.config.test_bucket}-markers'
        objects = ('d/', 'parent/', 'parent/dir/', 'parent/dir/file', 'parent/dir/sub/')

        def status(request, key, expected_code=None, **kwargs):
            try:
                response = request(Bucket=bucket, Key=key, **kwargs)
            except ClientError as error:
                if expected_code is not None:
                    assert error.response['Error']['Code'] == expected_code, error
                return error.response['ResponseMetadata']['HTTPStatusCode']
            if 'Body' in response:
                response['Body'].close()
            return response['ResponseMetadata']['HTTPStatusCode']

        def entries(**kwargs):
            result = s3.list_objects_v2(Bucket=bucket, **kwargs)
            return ([obj['Key'] for obj in result.get('Contents', [])],
                    [group['Prefix'] for group in result.get('CommonPrefixes', [])])

        s3.create_bucket(Bucket=bucket)
        try:
            for key in objects:
                s3.put_object(Bucket=bucket, Key=key, Body=b'x' if key.endswith('/file') else b'')

            file_key = 'parent/dir/file'
            file_etag = s3.head_object(Bucket=bucket, Key=file_key)['ETag']
            for bad in ('/', 'd//', '/d/', 'parent//dir/', f'{file_key}/'):
                assert status(s3.put_object, bad, 'InvalidArgument', Body=b'') == 400, bad
            file = s3.get_object(Bucket=bucket, Key=file_key)
            with file['Body'] as body:
                assert body.read() == b'x'
            assert file['ETag'] == file_etag
            for bad in ('d', 'd//', 'parent//dir/', f'{file_key}/'):
                assert status(s3.head_object, bad) == 404, bad
                assert status(s3.get_object, bad, 'NoSuchKey') == 404, bad
            assert status(s3.head_object, 'd', IfNoneMatch='*') == 404
            marker = s3.get_object(Bucket=bucket, Key='d/')
            with marker['Body'] as body:
                assert body.read() == b''
            assert marker['ContentLength'] == 0
            assert s3.head_object(Bucket=bucket, Key='d/')['ETag'] == marker['ETag']
            assert status(s3.head_object, 'd/', IfNoneMatch=marker['ETag']) == 304
            assert status(s3.get_object, 'd/', IfNoneMatch=marker['ETag']) == 304
            assert status(s3.head_object, 'd/', IfMatch='"stale"') == 412
            assert status(s3.get_object, 'd/', 'PreconditionFailed', IfMatch='"stale"') == 412
            assert entries(Prefix='parent/', Delimiter='/') == (['parent/'], ['parent/dir/'])
            assert entries(Delimiter='/') == ([], ['d/', 'parent/'])
            assert entries() == (list(objects), [])

            for alias in ('d', 'd//', 'parent/dir/file/'):
                assert status(s3.delete_object, alias) == 204, alias
            s3.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': 'parent//dir/'}]})
            assert entries() == (list(objects), [])
            assert status(s3.delete_object, 'd/') == 204
            assert status(s3.head_object, 'd/') == 404
            assert status(s3.delete_object, 'd/') == 204
            s3.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': 'parent/dir/'}]})
            assert status(s3.head_object, 'parent/dir/') == 404
            assert status(s3.head_object, 'parent/dir/file') == 200
        finally:
            for key in reversed(objects):
                s3.delete_object(Bucket=bucket, Key=key)
            s3.delete_bucket(Bucket=bucket)
        self._record_test('Directory marker keys and delimiter grouping', True)
        return True

    def test_list_object_versions(self) -> bool:
        """#2100: ListObjectVersions must traverse nested keys like ListObjectsV2."""
        s3, bucket = self.client.client, self.client.config.test_bucket
        keys = ['lver/' + k for k in ('a-', 'a/deep/1', 'a/deep/2', 'a/file', 'a0')]
        success = False

        def entries(operation: str, **kwargs) -> list:
            found = []
            for page in s3.get_paginator(operation).paginate(
                    PaginationConfig={'PageSize': 1}, **kwargs):
                versions = page.get('Versions', [])
                # A directory is a CommonPrefix, never an object version.
                assert not [v for v in versions if v['Key'].endswith('/')], \
                    'directory reported as a version'
                found += [i['Key'] for i in page.get('Contents', [])]
                found += [v['Key'] for v in versions]
                found += [i['Prefix'] for i in page.get('CommonPrefixes', [])]
            return found

        try:
            for key in keys:
                s3.put_object(Bucket=bucket, Key=key, Body=b'x')
            for prefix, delimiter in (('lver/', ''), ('lver/', '/'),
                                      ('lver/a/d', ''), ('lver/', 'deep')):
                args = {'Bucket': bucket, 'Prefix': prefix, 'Delimiter': delimiter}
                objects = entries('list_objects_v2', **args)
                versions = entries('list_object_versions', **args)
                assert sorted(versions) == sorted(objects), \
                    f'{prefix!r} {delimiter!r}: {versions} != {objects}'
            deep = s3.list_object_versions(Bucket=bucket, Prefix='lver/a/deep/')
            assert sorted(v['Key'] for v in deep.get('Versions', [])) == \
                ['lver/a/deep/1', 'lver/a/deep/2'], 'nested prefix listed only the root'

            # #2100 review: an explicit `EncodingType='url'` client re-encodes the
            # escaped marker it received, so `a b` comes back as `key-marker=a%2520b`.
            # Paging from that still-escaped value resumed after `a%20b` and dropped
            # `a!`, which sorts between the raw key and its escaped form. The
            # literal `%20` key keeps the ambiguity honest.
            tricky = ['enc/a b', 'enc/a!', 'enc/a%20b', 'enc/dir/']
            for key in tricky:
                s3.put_object(Bucket=bucket, Key=key, Body=b'' if key.endswith('/') else b'x')
            keys += tricky

            def encoded_pages(**kwargs):
                """Page with an explicit EncodingType=url and unescape the reply."""
                pages = list(s3.get_paginator('list_object_versions').paginate(
                    PaginationConfig={'PageSize': 1}, EncodingType='url', **kwargs))
                # An explicit EncodingType leaves the unescaping to the caller.
                found = [unquote(key)
                         for page in pages
                         for key in [v['Key'] for v in page.get('Versions', [])] +
                         [group['Prefix'] for group in page.get('CommonPrefixes', [])]]
                return found, len(pages)

            found, pages = encoded_pages(Bucket=bucket, Prefix='enc/')
            assert found == tricky, f'encoding-type=url paging lost keys: {found}'
            assert pages == len(tricky), f'PageSize=1 must page, got {pages} pages'
            folded, _ = encoded_pages(Bucket=bucket, Prefix='enc/', Delimiter='/')
            assert folded == ['enc/a b', 'enc/a!', 'enc/a%20b', 'enc/dir/'], \
                f'a directory marker was not folded into CommonPrefixes: {folded}'
            exact, _ = encoded_pages(Bucket=bucket, Prefix='enc/dir/')
            assert exact == ['enc/dir/'], \
                f'a marker matching the prefix must stay a version: {exact}'
            success = True
        except (ClientError, AssertionError) as error:
            print(f'ListObjectVersions regression failed: {error}')
        finally:
            for key in keys:
                try:
                    s3.delete_object(Bucket=bucket, Key=key)
                except ClientError:
                    pass
            self._record_test('ListObjectVersions nested keys and CommonPrefixes (#2100)', success)
        return success

    def _record_test(self, test_name: str, success: bool):
        """Record test result"""
        self.test_results.append((test_name, success))
        status = "✓" if success else "✗"
        print(f"  {status} {test_name}")


class S3RangeRequestTest:
    """Test S3 range requests"""
    
    def __init__(self, client: S3TestClient):
        self.client = client
        self.test_results = []
    
    def test_normal_range(self, key: str, start: int, end: int) -> Tuple[bool, Optional[bytes]]:
        """Test normal range request (bytes=start-end)"""
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Range=f'bytes={start}-{end}'
            )
            data = response['Body'].read()
            expected_size = end - start + 1
            success = len(data) == expected_size
            
            self._record_test(f"Range Request ({start}-{end})", success)
            return success, data
        except ClientError as e:
            print(f"Failed range request {start}-{end}: {e}")
            self._record_test(f"Range Request ({start}-{end})", False)
            return False, None
    
    def test_open_range(self, key: str, start: int) -> Tuple[bool, Optional[bytes]]:
        """Test open range request (bytes=start-)"""
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Range=f'bytes={start}-'
            )
            data = response['Body'].read()
            self._record_test(f"Open Range Request ({start}-)", True)
            return True, data
        except ClientError as e:
            print(f"Failed open range request {start}-: {e}")
            self._record_test(f"Open Range Request ({start}-)", False)
            return False, None
    
    def test_suffix_range(self, key: str, suffix_len: int) -> Tuple[bool, Optional[bytes]]:
        """Test suffix range request (bytes=-suffix_len)"""
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Range=f'bytes=-{suffix_len}'
            )
            data = response['Body'].read()
            success = len(data) == suffix_len
            
            self._record_test(f"Suffix Range Request (-{suffix_len})", success)
            return success, data
        except ClientError as e:
            print(f"Failed suffix range request -{suffix_len}: {e}")
            self._record_test(f"Suffix Range Request (-{suffix_len})", False)
            return False, None
    
    def test_invalid_range(self, key: str, start: int, end: int) -> bool:
        """Test invalid range request (should return 416)"""
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Range=f'bytes={start}-{end}'
            )
            # If we get here, the request succeeded when it shouldn't have
            self._record_test(f"Invalid Range Error ({start}-{end})", False)
            return False
        except ClientError as e:
            # Check if it's a 416 error
            success = e.response['Error']['Code'] == 'InvalidRange' or '416' in str(e)
            self._record_test(f"Invalid Range Error ({start}-{end})", success)
            return success
    
    def _record_test(self, test_name: str, success: bool):
        """Record test result"""
        self.test_results.append((test_name, success))
        status = "✓" if success else "✗"
        print(f"  {status} {test_name}")


class S3PerformanceTest:
    """Test S3 performance and streaming"""
    
    def __init__(self, client: S3TestClient):
        self.client = client
        self.test_results = []
    
    def test_large_file_upload(self, key: str, size_mb: int) -> Tuple[bool, float]:
        """Test large file upload performance"""
        data = TestDataGenerator.create_zero_data(size_mb * 1024 * 1024)
        
        start_time = time.time()
        try:
            self.client.client.put_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Body=data
            )
            upload_time = time.time() - start_time
            
            self._record_test(f"Large Upload ({size_mb}MB)", True)
            print(f"    Upload time: {upload_time:.2f}s ({size_mb/upload_time:.2f} MB/s)")
            return True, upload_time
        except ClientError as e:
            print(f"Failed large upload: {e}")
            self._record_test(f"Large Upload ({size_mb}MB)", False)
            return False, 0.0
    
    def test_large_file_download(self, key: str, expected_size: int) -> Tuple[bool, float]:
        """Test large file download performance"""
        start_time = time.time()
        try:
            response = self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key=key
            )
            data = response['Body'].read()
            download_time = time.time() - start_time
            
            success = len(data) == expected_size
            size_mb = expected_size / (1024 * 1024)
            
            self._record_test(f"Large Download ({size_mb:.0f}MB)", success)
            if success:
                print(f"    Download time: {download_time:.2f}s ({size_mb/download_time:.2f} MB/s)")
            return success, download_time
        except ClientError as e:
            print(f"Failed large download: {e}")
            self._record_test(f"Large Download", False)
            return False, 0.0
    
    def test_concurrent_operations(self, num_files: int, file_size: int) -> bool:
        """Test concurrent upload/download operations"""
        print(f"Testing concurrent operations with {num_files} files...")
        
        # Generate test data
        test_files = {}
        for i in range(num_files):
            key = f"concurrent-{i}.bin"
            data = TestDataGenerator.create_binary_data(file_size)
            test_files[key] = data
        
        # Concurrent upload
        upload_success = self._concurrent_upload(test_files)
        
        # Concurrent download
        download_success = self._concurrent_download(test_files) if upload_success else False
        
        # Cleanup
        self._cleanup_concurrent_files(list(test_files.keys()))
        
        success = upload_success and download_success
        self._record_test(f"Concurrent Operations ({num_files} files)", success)
        return success
    
    def _concurrent_upload(self, test_files: Dict[str, bytes]) -> bool:
        """Upload files concurrently"""
        def upload_file(key_data):
            key, data = key_data
            try:
                self.client.client.put_object(
                    Bucket=self.client.config.test_bucket,
                    Key=key,
                    Body=data
                )
                return True
            except ClientError:
                return False
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(upload_file, item) for item in test_files.items()]
            results = [future.result() for future in as_completed(futures)]
        
        return all(results)
    
    def _concurrent_download(self, test_files: Dict[str, bytes]) -> bool:
        """Download files concurrently and verify"""
        def download_file(key):
            try:
                response = self.client.client.get_object(
                    Bucket=self.client.config.test_bucket,
                    Key=key
                )
                return key, response['Body'].read()
            except ClientError:
                return key, None
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(download_file, key) for key in test_files.keys()]
            results = {key: data for key, data in [future.result() for future in as_completed(futures)]}
        
        # Verify all downloads match original data
        for key, original_data in test_files.items():
            downloaded_data = results.get(key)
            if downloaded_data != original_data:
                return False
        
        return True
    
    def _cleanup_concurrent_files(self, keys: List[str]):
        """Clean up concurrent test files"""
        for key in keys:
            try:
                self.client.client.delete_object(
                    Bucket=self.client.config.test_bucket,
                    Key=key
                )
            except ClientError:
                pass  # Ignore cleanup errors
    
    def _record_test(self, test_name: str, success: bool):
        """Record test result"""
        self.test_results.append((test_name, success))
        status = "✓" if success else "✗"
        print(f"  {status} {test_name}")


class S3ErrorHandlingTest:
    """Test S3 error handling"""
    
    def __init__(self, client: S3TestClient):
        self.client = client
        self.test_results = []
    
    def test_nonexistent_object_error(self) -> bool:
        """Test 404 error for non-existent object"""
        try:
            # GET, not HEAD: a HEAD response has no body to carry the code.
            self.client.client.get_object(
                Bucket=self.client.config.test_bucket,
                Key='non-existent-object.txt'
            )
            # If we get here, it didn't fail as expected
            self._record_test("404 Error (Non-existent Object)", False)
            return False
        except ClientError as e:
            success = e.response['Error']['Code'] == 'NoSuchKey'
            self._record_test("404 Error (Non-existent Object)", success)
            return success
    
    def test_nonexistent_bucket_error(self) -> bool:
        """Test 404 error for non-existent bucket"""
        try:
            self.client.client.head_bucket(Bucket='non-existent-bucket-12345')
            # If we get here, it didn't fail as expected
            self._record_test("404 Error (Non-existent Bucket)", False)
            return False
        except ClientError as e:
            success = e.response['Error']['Code'] in ['NoSuchBucket', '404']
            self._record_test("404 Error (Non-existent Bucket)", success)
            return success
    
    def test_multipart_error_codes(self) -> bool:
        """#2092: SDKs read the S3 error code, not only the HTTP status."""
        s3, bucket, key = self.client.client, self.client.config.test_bucket, 'mp-errors.bin'

        def code(parts):
            try:
                s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload,
                                             MultipartUpload={'Parts': parts})
            except ClientError as e:
                return e.response['ResponseMetadata']['HTTPStatusCode'], e.response['Error']['Code']

        upload = s3.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
        parts = [{'PartNumber': n, 'ETag': s3.upload_part(
            Bucket=bucket, Key=key, UploadId=upload, PartNumber=n, Body=b'x' * size)['ETag']}
            for n, size in ((1, 5 << 20), (2, 1), (3, 1))]
        observed = [code(parts[::-1]), code([dict(parts[0], ETag='"00"')]), code(parts[1:])]
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
        observed.append(code(parts[:1]))
        success = observed == [(400, 'InvalidPartOrder'), (400, 'InvalidPart'),
                               (400, 'EntityTooSmall'), (404, 'NoSuchUpload')]

        # #2184: repeating Complete with the same parts replays the ETag; a
        # mismatched ETag or part number is InvalidPart; a copy never inherits
        # the upload receipt.
        upload = s3.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
        parts = [{'PartNumber': 1, 'ETag': s3.upload_part(
            Bucket=bucket, Key=key, UploadId=upload, PartNumber=1, Body=b'y')['ETag']}]
        first = s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload,
                                             MultipartUpload={'Parts': parts})['ETag']
        try:
            again = s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload,
                                                 MultipartUpload={'Parts': parts})['ETag']
        except ClientError:
            again = None
        s3.copy_object(Bucket=bucket, Key='mp-copy.bin', CopySource={'Bucket': bucket, 'Key': key})
        copy_err = None
        try:
            s3.complete_multipart_upload(Bucket=bucket, Key='mp-copy.bin', UploadId=upload,
                                         MultipartUpload={'Parts': parts})
        except ClientError as e:
            copy_err = (e.response['ResponseMetadata']['HTTPStatusCode'], e.response['Error']['Code'])
        replay = (first == again,
                  code([dict(parts[0], ETag='"00"')]),
                  code([dict(parts[0], PartNumber=2)]),
                  copy_err)
        success = success and replay == (True, (400, 'InvalidPart'), (400, 'InvalidPart'),
                                         (404, 'NoSuchUpload'))
        self._record_test(f"Multipart error codes {observed} replay={replay}", success)
        return success

    def test_auth_error_codes(self) -> bool:
        """#2102: authentication failures carry S3 codes, not the numeric status."""
        config = self.client.config

        def code(access_key: str, secret_key: str):
            bad = boto3.client(
                's3',
                endpoint_url=config.endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=config.region,
                config=Config(signature_version='s3v4', retries={'max_attempts': 1}),
            )
            try:
                bad.list_objects_v2(Bucket=config.test_bucket)
            except ClientError as e:
                return (e.response['ResponseMetadata']['HTTPStatusCode'],
                        e.response['Error']['Code'])
            return (200, 'OK')

        observed = [code('CURVINENOSUCHKEY', 'secret'),
                    code(config.access_key, 'wrong-secret')]
        success = observed == [(403, 'InvalidAccessKeyId'), (403, 'SignatureDoesNotMatch')]
        self._record_test(f"Auth error codes {observed}", success)
        return success

    def test_upload_part_error_codes(self) -> bool:
        """#2102: an unknown or aborted upload id answers NoSuchUpload, not 500."""
        s3, bucket, key = self.client.client, self.client.config.test_bucket, 'mp-part-errors.bin'

        def part_code(upload_id):
            try:
                s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id,
                               PartNumber=1, Body=b'x')
            except ClientError as e:
                return (e.response['ResponseMetadata']['HTTPStatusCode'],
                        e.response['Error']['Code'])
            return (200, 'OK')

        def abort_code(upload_id):
            try:
                s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            except ClientError as e:
                return (e.response['ResponseMetadata']['HTTPStatusCode'],
                        e.response['Error']['Code'])
            return (204, 'NoContent')

        upload = s3.create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
        observed = [part_code('no-such-upload'), part_code(upload), abort_code(upload)]
        success = observed == [(404, 'NoSuchUpload')] * 3
        self._record_test(f"Upload part error codes {observed}", success)
        return success

    def test_duplicate_bucket_error(self) -> bool:
        """Test 409 error for duplicate bucket creation"""
        try:
            # Try to create the same bucket again
            self.client.client.create_bucket(Bucket=self.client.config.test_bucket)
            # If we get here, it didn't fail as expected
            self._record_test("409 Error (Duplicate Bucket)", False)
            return False
        except ClientError as e:
            success = e.response['Error']['Code'] in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']
            self._record_test("409 Error (Duplicate Bucket)", success)
            return success
    
    def _record_test(self, test_name: str, success: bool):
        """Record test result"""
        self.test_results.append((test_name, success))
        status = "✓" if success else "✗"
        print(f"  {status} {test_name}")


class S3MetadataTest:
    """Test S3 metadata handling"""
    
    def __init__(self, client: S3TestClient):
        self.client = client
        self.test_results = []
    
    def test_custom_metadata(self, key: str, metadata: Dict[str, str]) -> bool:
        """Test custom metadata storage and retrieval"""
        # Upload with metadata
        try:
            self.client.client.put_object(
                Bucket=self.client.config.test_bucket,
                Key=key,
                Body=b'test data with metadata',
                Metadata=metadata
            )
        except ClientError as e:
            print(f"Failed to upload with metadata: {e}")
            self._record_test("Custom Metadata Upload", False)
            return False
        
        # Retrieve and verify metadata
        try:
            response = self.client.client.head_object(
                Bucket=self.client.config.test_bucket,
                Key=key
            )
            retrieved_metadata = response.get('Metadata', {})
            
            # Exactly the uploaded metadata: no internal fields injected (#2094)
            success = retrieved_metadata == metadata
            self._record_test("Custom Metadata Retrieval", success)
            
            if success:
                print(f"    Metadata verified: {retrieved_metadata}")
            
            return success
        except ClientError as e:
            print(f"Failed to retrieve metadata: {e}")
            self._record_test("Custom Metadata Retrieval", False)
            return False
    
    def _record_test(self, test_name: str, success: bool):
        """Record test result"""
        self.test_results.append((test_name, success))
        status = "✓" if success else "✗"
        print(f"  {status} {test_name}")


class ComprehensiveS3TestSuite:
    """Main test suite coordinator"""
    
    def __init__(self):
        self.config = S3TestConfig()
        self.client = S3TestClient(self.config)
        self.all_results = []
    
    def run_all_tests(self) -> bool:
        """Run all test suites"""
        print("=== Comprehensive S3 Test Suite (Python/boto3) ===")
        print(f"Endpoint: {self.config.endpoint_url}")
        print(f"Test Bucket: {self.config.test_bucket}")
        print(f"Temp Directory: {self.config.temp_dir}")
        print()
        
        try:
            # Run test suites
            basic_success = self._run_basic_operations_tests()
            range_success = self._run_range_request_tests()
            performance_success = self._run_performance_tests()
            error_success = self._run_error_handling_tests()
            metadata_success = self._run_metadata_tests()
            
            # Cleanup
            self._cleanup()
            
            # Generate summary
            overall_success = all([
                basic_success, range_success, performance_success,
                error_success, metadata_success
            ])
            
            self._print_summary()
            return overall_success
            
        except Exception as e:
            print(f"Test suite failed with exception: {e}")
            return False
    
    def _run_basic_operations_tests(self) -> bool:
        """Run basic S3 operations tests"""
        print("=== Basic S3 Operations Tests ===")
        
        basic_test = S3BasicOperationsTest(self.client)
        
        # Test bucket operations
        if not basic_test.test_bucket_operations():
            return False
        
        # Test various data types
        test_data = {
            'small.txt': TestDataGenerator.create_text_data(100),
            'empty.txt': TestDataGenerator.create_empty_data(),
            'binary.bin': TestDataGenerator.create_binary_data(1024),
            'special.txt': TestDataGenerator.create_special_chars_data(),
            'medium.bin': TestDataGenerator.create_zero_data(1024 * 1024)  # 1MB
        }
        
        # Upload all test files
        for key, data in test_data.items():
            if not basic_test.test_put_object(key, data):
                return False
        
        # Test head operations
        for key in test_data.keys():
            if not basic_test.test_head_object(key):
                return False

        # HEAD applies response-* overrides like GET (#2185)
        if not basic_test.test_head_response_overrides('small.txt'):
            return False

        # Query-string SigV4 (#2093)
        if not basic_test.test_presigned_get():
            return False

        # UploadPartCopy (#2187)
        if not basic_test.test_upload_part_copy():
            return False
        
        # Test download and verify
        for key, original_data in test_data.items():
            success, downloaded_data = basic_test.test_get_object(key)
            if not success or downloaded_data != original_data:
                print(f"Data mismatch for {key}")
                return False
        
        if not basic_test.test_list_multipart_uploads():
            return False

        # A failed overwrite must not destroy the existing object
        if not basic_test.test_overwrite_is_atomic("overwrite-object"):
            return False

        # ListParts (#2223)
        if not basic_test.test_list_parts():
            return False

        # Required regression: nested keys and delimiter groups share pagination.
        if not basic_test.test_list_objects_v2_pagination():
            return False

        if not basic_test.test_directory_markers():
            return False

        # Required regression: ListObjectVersions sees the same keys as V2.
        if not basic_test.test_list_object_versions():
            return False

        # Test list operations
        success, objects = basic_test.test_list_objects()
        if not success or len(objects) != len(test_data):
            return False
        
        # Test list with prefix
        success, filtered_objects = basic_test.test_list_objects(prefix='s')
        if not success:
            return False
        
        self.all_results.extend(basic_test.test_results)
        print()
        return True
    
    def _run_range_request_tests(self) -> bool:
        """Run range request tests"""
        print("=== Range Request Tests ===")
        
        range_test = S3RangeRequestTest(self.client)
        
        # Create test file with known content
        test_data = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"  # 36 bytes
        key = "range-test.txt"
        
        # Upload test file
        try:
            self.client.client.put_object(
                Bucket=self.config.test_bucket,
                Key=key,
                Body=test_data
            )
        except ClientError as e:
            print(f"Failed to upload range test file: {e}")
            return False
        
        # Test normal ranges
        success, data = range_test.test_normal_range(key, 0, 4)
        if not success or data != b"ABCDE":
            return False
        
        success, data = range_test.test_normal_range(key, 10, 15)
        if not success or data != b"KLMNOP":
            return False
        
        # Test open range
        success, data = range_test.test_open_range(key, 30)
        if not success or data != b"456789":
            return False
        
        # Test suffix range
        success, data = range_test.test_suffix_range(key, 5)
        if not success or data != b"56789":
            return False
        
        # Test invalid range
        success = range_test.test_invalid_range(key, 100, 200)
        if not success:
            return False
        
        self.all_results.extend(range_test.test_results)
        print()
        return True
    
    def _run_performance_tests(self) -> bool:
        """Run performance and streaming tests"""
        print("=== Performance and Streaming Tests ===")
        
        perf_test = S3PerformanceTest(self.client)
        
        # Test large file upload/download (10MB)
        key = "large-test.bin"
        size_mb = 10
        
        success, upload_time = perf_test.test_large_file_upload(key, size_mb)
        if not success:
            return False
        
        expected_size = size_mb * 1024 * 1024
        success, download_time = perf_test.test_large_file_download(key, expected_size)
        if not success:
            return False
        
        # Test concurrent operations
        success = perf_test.test_concurrent_operations(num_files=5, file_size=1024)
        if not success:
            return False
        
        self.all_results.extend(perf_test.test_results)
        print()
        return True
    
    def _run_error_handling_tests(self) -> bool:
        """Run error handling tests"""
        print("=== Error Handling Tests ===")
        
        error_test = S3ErrorHandlingTest(self.client)
        
        # Test various error conditions
        if not error_test.test_nonexistent_object_error():
            return False
        
        if not error_test.test_nonexistent_bucket_error():
            return False
        
        if not error_test.test_duplicate_bucket_error():
            return False

        if not error_test.test_multipart_error_codes():
            return False

        if not error_test.test_auth_error_codes():
            return False

        if not error_test.test_upload_part_error_codes():
            return False
        
        self.all_results.extend(error_test.test_results)
        print()
        return True
    
    def _run_metadata_tests(self) -> bool:
        """Run metadata tests"""
        print("=== Metadata Tests ===")
        
        metadata_test = S3MetadataTest(self.client)
        
        # Test custom metadata
        metadata = {
            'author': 'curvine-test',
            'version': '1.0',
            'test-key': 'test-value'
        }
        
        success = metadata_test.test_custom_metadata('metadata-test.txt', metadata)
        
        self.all_results.extend(metadata_test.test_results)
        print()
        return success
    
    def _cleanup(self):
        """Clean up test resources"""
        print("=== Cleanup ===")
        
        # Delete all objects in bucket
        try:
            response = self.client.client.list_objects_v2(Bucket=self.config.test_bucket)
            if 'Contents' in response:
                for obj in response['Contents']:
                    self.client.client.delete_object(
                        Bucket=self.config.test_bucket,
                        Key=obj['Key']
                    )
        except ClientError:
            pass  # Ignore cleanup errors
        
        # Delete bucket
        self.client.delete_bucket()
        
        # Clean up temp directory
        import shutil
        try:
            shutil.rmtree(self.config.temp_dir)
        except OSError:
            pass  # Ignore cleanup errors
        
        print("Cleanup completed")
        print()
    
    def _print_summary(self):
        """Print test summary"""
        print("=== Test Suite Summary ===")
        
        passed = sum(1 for _, success in self.all_results if success)
        failed = sum(1 for _, success in self.all_results if not success)
        total = len(self.all_results)
        
        print(f"Tests Passed: {passed}")
        print(f"Tests Failed: {failed}")
        print(f"Total Tests: {total}")
        print()
        
        if failed == 0:
            print("🎉 SUCCESS: All Python S3 tests passed!")
            print()
            print("✅ Verified Features:")
            print("   - Basic S3 Operations (PUT, GET, HEAD, DELETE, LIST)")
            print("   - Bucket Management (CREATE, DELETE, HEAD)")
            print("   - Range Requests (normal, open, suffix)")
            print("   - Large File Streaming (10MB+)")
            print("   - Concurrent Operations")
            print("   - Error Handling (404, 409, 416)")
            print("   - Custom Metadata")
            print("   - Various Data Types (text, binary, empty, special chars)")
            print()
            print("🚀 Curvine S3 Gateway Python compatibility confirmed!")
        else:
            print("⚠️ PARTIAL SUCCESS: Some tests failed")
            print()
            print("❌ Failed Tests:")
            for test_name, success in self.all_results:
                if not success:
                    print(f"   - {test_name}")
            print()
            print("🔧 Please review and fix the failed components.")


def main():
    """Main entry point"""
    test_suite = ComprehensiveS3TestSuite()
    success = test_suite.run_all_tests()
    
    print(f"Test completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 