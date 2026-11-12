#!/usr/bin/env python3
# Copyright 2026 OPPO.
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
"""Start an isolated Master + Worker + S3 gateway and verify real S3 I/O.

Requires Python >= 3.10, boto3, and release binaries. No existing cluster or
credentials are used. Each run gets fresh storage and random local credentials.
Processes are stopped in finally; logs/data/results remain for inspection.
Compatibility probes are reported separately, never counted as core passes.
"""

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import tempfile
import time
import traceback
import urllib.request

import boto3
import botocore
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

MIB = 1024 * 1024
ROOT = Path(__file__).resolve().parents[3]


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def md5(data):
    return hashlib.md5(data).hexdigest()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def expect_error(call, status):
    try:
        call()
    except ClientError as error:
        actual = error.response['ResponseMetadata']['HTTPStatusCode']
        require(actual == status, f'expected HTTP {status}, got {actual}: {error}')
        return {'http_status': actual, 'code': error.response['Error']['Code']}
    raise AssertionError(f'expected HTTP {status}, request succeeded')


class Suite:
    def __init__(self, output, binary_dir):
        self.output = output
        self.binary_dir = binary_dir
        self.processes = []
        self.results = []
        self.probes = []
        self.uploads = set()
        self.bucket = 'curvine-e2e-' + secrets.token_hex(4)
        self.access = 'E2E' + secrets.token_hex(10).upper()
        self.secret = secrets.token_hex(24)
        self.started = time.time()
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.ports = {}
        self.client = None

    def record(self, name, call, probe=False):
        start = time.monotonic()
        row = {'name': name}
        try:
            row['detail'] = call() or {}
            row['status'] = 'PASS'
        except Exception as error:
            row['status'] = 'LIMITATION' if probe else 'FAIL'
            row['error'] = str(error)
            if not probe:
                traceback.print_exc()
        row['seconds'] = round(time.monotonic() - start, 4)
        (self.probes if probe else self.results).append(row)
        print(f"{row['status']:10} {name}: {row.get('error', row.get('detail'))}", flush=True)
        return row['status'] == 'PASS'

    def fetch(self, url):
        with self.opener.open(url, timeout=2) as response:
            return response.read().decode()

    def wait(self, label, check, timeout=90):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            for name, process, _ in self.processes:
                if process.poll() is not None:
                    raise RuntimeError(f'{name} exited ({process.returncode}); see {self.output}/{name}.log')
            try:
                if check():
                    print(f'READY {label}', flush=True)
                    return
            except Exception as error:
                last = error
            time.sleep(0.25)
        raise TimeoutError(f'{label} not ready: {last}')

    def spawn(self, name, command, env):
        log = open(self.output / f'{name}.log', 'w')
        process = subprocess.Popen(command, cwd=self.output, env=env, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        self.processes.append((name, process, log))
        (self.output / f'{name}.pid').write_text(str(process.pid) + '\n')

    def start(self):
        # Keep reservations open until every distinct port has been selected.
        sockets = []
        try:
            for name in ('master_rpc', 'journal', 'worker_rpc', 'master_web',
                         'worker_web', 's3', 's3_web'):
                sock = socket.socket()
                sock.bind(('127.0.0.1', 0))
                sockets.append(sock)
                self.ports[name] = sock.getsockname()[1]
        finally:
            for sock in sockets:
                sock.close()
        p = self.ports
        self.endpoint = f"http://127.0.0.1:{p['s3']}"
        self.conf = self.output / 'cluster.toml'
        # Refuse destructive format-on-restart, even inside this fresh directory.
        self.conf.write_text(f'''format_master = false
format_worker = false
cluster_id = "s3-e2e-local"
[master]
hostname = "127.0.0.1"
rpc_port = {p['master_rpc']}
web_port = {p['master_web']}
meta_dir = "{self.output}/meta"
io_threads = 2
worker_threads = 4
actor_threads = 2
executor_threads = 2
min_replication = 1
max_replication = 100
heartbeat_interval = "1s"
log = {{ level = "info", log_dir = "stdout", file_name = "master.log" }}
[master.rocksdb]
block_cache_size = "128MB"
max_background_jobs = 2
[journal]
hostname = "127.0.0.1"
rpc_port = {p['journal']}
journal_addrs = [{{ id = 1, hostname = "127.0.0.1", port = {p['journal']} }}]
journal_dir = "{self.output}/journal"
io_threads = 1
worker_threads = 2
[worker]
hostname = "127.0.0.1"
rpc_port = {p['worker_rpc']}
web_port = {p['worker_web']}
data_dir = ["[DISK:2GB]{self.output}/data"]
dir_reserved = "0"
io_threads = 2
worker_threads = 4
executor_threads = 2
log = {{ level = "info", log_dir = "stdout", file_name = "worker.log" }}
[client]
hostname = "127.0.0.1"
master_addrs = [{{ hostname = "127.0.0.1", port = {p['master_rpc']} }}]
io_threads = 2
worker_threads = 4
replicas = 1
[s3_gateway]
listen = "127.0.0.1:{p['s3']}"
web_port = {p['s3_web']}
region = "us-east-1"
credentials_path = "{self.output}/credentials.jsonl"
put_temp_dir = "{self.output}/parts"
enable_distributed_auth = false
''')
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(('AWS_', 'CURVINE_'))}
        env['RUST_LOG'] = 'info'
        server = str(self.binary_dir / 'curvine-server')
        self.spawn('master', [server, '--service', 'master', '--conf', str(self.conf)], env)
        self.wait('Master web', lambda: self.fetch(f"http://127.0.0.1:{p['master_web']}/api/workers"))
        self.spawn('worker', [server, '--service', 'worker', '--conf', str(self.conf)], env)
        self.wait('Worker registered', lambda: len(json.loads(self.fetch(
            f"http://127.0.0.1:{p['master_web']}/api/workers"))['live_workers']) == 1)
        gateway_env = dict(env, CURVINE_ACCESS_KEY=self.access, CURVINE_SECRET_KEY=self.secret)
        gateway = str(self.binary_dir / 'curvine-s3-gateway')
        self.spawn('gateway', [gateway, '--conf', str(self.conf), 'serve'], gateway_env)
        self.wait('S3 gateway', lambda: self.fetch(self.endpoint + '/healthz') == 'ok')
        self.client = self.make_client(compatible=True)
        self.client.create_bucket(Bucket=self.bucket)
        self.client.head_bucket(Bucket=self.bucket)
        return {'live_workers': 1, 'endpoint': self.endpoint, 'bucket': self.bucket}

    def make_client(self, compatible):
        options = dict(signature_version='s3v4', s3={'addressing_style': 'path'},
                       retries={'total_max_attempts': 1}, connect_timeout=5, read_timeout=30,
                       proxies={})
        if compatible:
            # Keep optional checksum/framing behavior out of the core tests;
            # a separate probe exercises the SDK defaults over local HTTP.
            options.update(request_checksum_calculation='when_required',
                           response_checksum_validation='when_required')
        return boto3.client('s3', endpoint_url=self.endpoint, region_name='us-east-1',
                            aws_access_key_id=self.access, aws_secret_access_key=self.secret,
                            config=Config(**options))

    def get(self, key, **kwargs):
        response = self.client.get_object(Bucket=self.bucket, Key=key, **kwargs)
        try:
            data = response['Body'].read()
        finally:
            response['Body'].close()
        return response, data

    def roundtrip(self, key, data):
        start = time.monotonic()
        put = self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        put_seconds = time.monotonic() - start
        head = self.client.head_object(Bucket=self.bucket, Key=key)
        start = time.monotonic()
        response, downloaded = self.get(key)
        get_seconds = time.monotonic() - start
        require(downloaded == data, 'downloaded bytes differ')
        require(head['ContentLength'] == len(data), 'HEAD length differs')
        require(put['ETag'] == head['ETag'] == response['ETag'] == f'"{md5(data)}"',
                'PUT/HEAD/GET ETag differs from content MD5')
        return {'bytes': len(data), 'sha256': sha256(downloaded),
                'put_seconds': round(put_seconds, 4), 'get_seconds': round(get_seconds, 4)}

    def create_upload(self, key):
        upload = self.client.create_multipart_upload(Bucket=self.bucket, Key=key)['UploadId']
        self.uploads.add((key, upload))
        return upload

    def part(self, key, upload, number, data):
        response = self.client.upload_part(Bucket=self.bucket, Key=key, UploadId=upload,
                                          PartNumber=number, Body=data)
        require(response['ETag'].strip('"') == md5(data), 'part ETag differs from MD5')
        return {'PartNumber': number, 'ETag': response['ETag']}

    def complete(self, key, upload, parts):
        response = self.client.complete_multipart_upload(
            Bucket=self.bucket, Key=key, UploadId=upload, MultipartUpload={'Parts': parts})
        self.uploads.discard((key, upload))
        return response

    def multipart(self, reject=None):
        key = f'multipart-{reject or "normal"}.bin'
        chunks = [os.urandom(5 * MIB), os.urandom(5 * MIB), os.urandom(MIB + 123)]
        old = b'previous-object-must-survive-rejected-complete'
        if reject:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=old)
        upload = self.create_upload(key)
        parts = [self.part(key, upload, i + 1, chunk) for i, chunk in enumerate(chunks)]
        rejected = None
        if reject:
            invalid = list(reversed(parts)) if reject == 'order' else [dict(p) for p in parts]
            if reject == 'etag':
                invalid[0]['ETag'] = '"' + '0' * 32 + '"'
            rejected = expect_error(lambda: self.client.complete_multipart_upload(
                Bucket=self.bucket, Key=key, UploadId=upload,
                MultipartUpload={'Parts': invalid}), 400)
            require(self.get(key)[1] == old, 'rejected Complete changed the old object')
        completed = self.complete(key, upload, parts)
        expected = b''.join(chunks)
        response, data = self.get(key)
        expected_etag = '"' + md5(b''.join(bytes.fromhex(md5(c)) for c in chunks)) + '-3"'
        head = self.client.head_object(Bucket=self.bucket, Key=key)
        require(data == expected, 'assembled object content differs')
        require(completed['ETag'] == response['ETag'] == head['ETag'] == expected_etag,
                'multipart ETag differs across Complete/GET/HEAD')
        require(head['ContentLength'] == len(expected), 'multipart size differs')
        require(not (self.output / 'parts' / upload).exists(), 'completed parts not cleaned')
        return {'parts': 3, 'bytes': len(data), 'sha256': sha256(data), 'etag': expected_etag,
                'rejected_complete': rejected, 'retry_without_reupload': bool(reject)}

    def run_tests(self):
        s3 = self.client
        b = self.bucket
        self.record('empty object PUT/HEAD/GET', lambda: self.roundtrip('empty.bin', b''))
        self.record('small text PUT/HEAD/GET', lambda: self.roundtrip('text.txt', b'Curvine S3 end-to-end\n'))
        self.record('1 MiB binary PUT/HEAD/GET', lambda: self.roundtrip('one-mib.bin', os.urandom(MIB)))
        self.record('32 MiB binary PUT/HEAD/GET', lambda: self.roundtrip('large.bin', os.urandom(32 * MIB)))
        self.record('percent-encoded Unicode object key', lambda: self.roundtrip(
            '中文 空格+百分号%25.txt', '对象内容\n'.encode()))

        def metadata():
            s3.put_object(Bucket=b, Key='metadata.txt', Body=b'metadata-body',
                          ContentType='text/plain', Metadata={'purpose': 'e2e', 'owner': 'local'})
            observed = []
            for response in (s3.head_object(Bucket=b, Key='metadata.txt'), self.get('metadata.txt')[0]):
                for name, value in {'purpose': 'e2e', 'owner': 'local'}.items():
                    require(response['Metadata'].get(name) == value, f'user metadata {name} differs')
                require(response['ContentType'] == 'text/plain', 'content type differs')
                observed.append(sorted(response['Metadata']))
            return {'content_type': 'text/plain', 'head_and_get_metadata_keys': observed}
        self.record('Content-Type and supplied metadata fields', metadata)

        def exact_metadata():
            # Keep strict S3 compatibility as a real failure, separate from
            # checking whether the supplied user fields survived the upload.
            for method, response in (
                ('HEAD', s3.head_object(Bucket=b, Key='metadata.txt')),
                ('GET', self.get('metadata.txt')[0]),
            ):
                extra = sorted(set(response['Metadata']) - {'purpose', 'owner'})
                require(not extra, f'{method} injected metadata keys: {extra}')
        self.record('exact S3 Metadata map (no injected fields)', exact_metadata)

        def overwrite():
            self.roundtrip('overwrite.bin', b'old')
            return self.roundtrip('overwrite.bin', b'new object with a different size')
        self.record('overwrite existing object', overwrite)

        ranged = bytes(range(256)) * 4096
        s3.put_object(Bucket=b, Key='range.bin', Body=ranged)
        for name, header, expected in (
            ('closed Range', 'bytes=17-1016', ranged[17:1017]),
            ('open Range', 'bytes=1000000-', ranged[1000000:]),
            ('suffix Range', 'bytes=-1234', ranged[-1234:]),
        ):
            def check_range(header=header, expected=expected):
                response, data = self.get('range.bin', Range=header)
                require(response['ResponseMetadata']['HTTPStatusCode'] == 206, 'expected 206')
                require(data == expected, 'Range bytes differ')
                require(response['ContentLength'] == len(expected), 'Range size differs')
                return {'bytes': len(data), 'content_range': response['ContentRange']}
            self.record(name, check_range)
        self.record('unsatisfiable Range -> 416', lambda: expect_error(
            lambda: self.get('range.bin', Range='bytes=2000000-'), 416))
        self.record('conditional GET If-None-Match -> 304', lambda: expect_error(
            lambda: self.get('range.bin', IfNoneMatch=s3.head_object(Bucket=b, Key='range.bin')['ETag']), 304))
        self.record('missing object -> 404', lambda: expect_error(lambda: self.get('does-not-exist'), 404))

        def copy():
            response = s3.copy_object(Bucket=b, Key='copied.bin', CopySource={'Bucket': b, 'Key': 'range.bin'})
            require(self.get('copied.bin')[1] == ranged, 'copy bytes differ')
            return {'etag': response['CopyObjectResult']['ETag']}
        self.record('server-side CopyObject', copy)
        self.record('manual multipart 5+5+1 MiB', self.multipart)
        self.record('invalid part order; preserve old object; retry Complete', lambda: self.multipart('order'))
        self.record('invalid part ETag; preserve old object; retry Complete', lambda: self.multipart('etag'))

        def abort():
            key = 'aborted.bin'
            upload = self.create_upload(key)
            self.part(key, upload, 1, b'part-to-abort')
            s3.abort_multipart_upload(Bucket=b, Key=key, UploadId=upload)
            self.uploads.discard((key, upload))
            require(not (self.output / 'parts' / upload).exists(), 'aborted parts not cleaned')
            expect_error(lambda: self.get(key), 404)
            return expect_error(lambda: self.complete(key, upload, [{'PartNumber': 1, 'ETag': '"none"'}]), 404)
        self.record('AbortMultipartUpload and dead upload -> 404', abort)

        def too_small():
            key = 'too-small.bin'
            upload = self.create_upload(key)
            parts = [self.part(key, upload, 1, b'small'), self.part(key, upload, 2, b'last')]
            result = expect_error(lambda: self.complete(key, upload, parts), 400)
            expect_error(lambda: self.get(key), 404)
            require((self.output / 'parts' / upload).exists(), 'rejected parts should be retained')
            s3.abort_multipart_upload(Bucket=b, Key=key, UploadId=upload)
            self.uploads.discard((key, upload))
            return result
        self.record('undersized non-final part -> 400; no object published', too_small)

        def managed_transfer():
            source = self.output / 'managed-source.bin'
            downloaded = self.output / 'managed-downloaded.bin'
            data = os.urandom(17 * MIB + 321)
            source.write_bytes(data)
            config = TransferConfig(multipart_threshold=5 * MIB, multipart_chunksize=5 * MIB, max_concurrency=3)
            s3.upload_file(str(source), b, 'managed.bin', Config=config)
            s3.download_file(b, 'managed.bin', str(downloaded), Config=config)
            require(sha256(downloaded.read_bytes()) == sha256(data), 'managed transfer SHA256 differs')
            return {'bytes': len(data), 'sha256': sha256(data), 'etag': s3.head_object(Bucket=b, Key='managed.bin')['ETag']}
        self.record('boto3 upload_file/download_file concurrent multipart', managed_transfer)

        def concurrent_roundtrips():
            def one(index):
                return self.roundtrip(f'parallel-{index:02d}.bin', os.urandom(256 * 1024))
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                values = list(pool.map(one, range(8)))
            return {'objects': len(values), 'bytes_each': 256 * 1024, 'workers': 4}
        self.record('8 objects concurrent PUT/GET', concurrent_roundtrips)

        def paginate():
            keys = []
            pages = 0
            for page in s3.get_paginator('list_objects_v2').paginate(Bucket=b, Prefix='parallel-', PaginationConfig={'PageSize': 3}):
                keys.extend(item['Key'] for item in page.get('Contents', []))
                pages += 1
                require(pages < 10, 'pagination did not terminate')
            require(keys == [f'parallel-{i:02d}.bin' for i in range(8)], f'pagination keys differ: {keys}')
            return {'pages': pages, 'objects': len(keys)}
        self.record('ListObjectsV2 pagination', paginate)

        def delete():
            s3.put_object(Bucket=b, Key='delete-me', Body=b'delete')
            s3.delete_object(Bucket=b, Key='delete-me')
            return expect_error(lambda: self.get('delete-me'), 404)
        self.record('DeleteObject', delete)

        def bucket_lifecycle():
            empty = b + '-empty'
            s3.create_bucket(Bucket=empty)
            s3.head_bucket(Bucket=empty)
            s3.delete_bucket(Bucket=empty)
            return expect_error(lambda: s3.head_bucket(Bucket=empty), 404)
        self.record('empty bucket create/head/delete', bucket_lifecycle)

    def listing_regressions(self):
        s3, b = self.client, self.bucket
        prefix = 'listing-2091/'
        relative = ['.visible', 'a-', 'a/deep/1', 'a/deep/2', 'a/file', 'a0',
                    'b/x:y', 'b/x:z', 'root', '中文/空格+百分号%.txt']
        objects = [prefix + key for key in relative]
        for key in objects:
            s3.put_object(Bucket=b, Key=key, Body=b'listing-regression')

        def recursive():
            result = s3.list_objects_v2(Bucket=b, Prefix=prefix)
            keys = [item['Key'] for item in result.get('Contents', [])]
            require(keys == sorted(objects), f'recursive listing differs: {keys}')
            require(not result.get('CommonPrefixes'), 'unexpected groups without delimiter')
            return {'objects': len(keys)}
        self.record('recursive ListObjectsV2 (required)', recursive)

        def prefixes():
            for suffix in ('a/d', 'a/deep/', 'a/deep/1', '中文/空格+'):
                result = s3.list_objects_v2(Bucket=b, Prefix=prefix + suffix)
                actual = [item['Key'] for item in result.get('Contents', [])]
                require(actual == sorted(k for k in objects if k.startswith(prefix + suffix)),
                        f'string prefix differs for {suffix}: {actual}')
            for suffix in ('missing/', 'a/missing/', 'does-not-exist'):
                result = s3.list_objects_v2(Bucket=b, Prefix=prefix + suffix)
                require(not result.get('Contents') and not result.get('CommonPrefixes'),
                        f'missing prefix was not empty: {suffix}')
        self.record('string, Unicode and missing prefixes', prefixes)

        def mixed_pages():
            requests = 0
            for delim in ('', '/', ':', 'deep'):
                expected = set()
                for key in objects:
                    rest = key[len(prefix):]
                    at = rest.find(delim) if delim else -1
                    expected.add(prefix + rest[:at + len(delim)] if at >= 0 else key)
                for size in (1, 3):
                    actual, token, seen = [], None, set()
                    while True:
                        args = dict(Bucket=b, Prefix=prefix, MaxKeys=size, Delimiter=delim)
                        if token:
                            args['ContinuationToken'] = token
                        page = s3.list_objects_v2(**args)
                        requests += 1
                        entries = [i['Key'] for i in page.get('Contents', [])]
                        entries += [i['Prefix'] for i in page.get('CommonPrefixes', [])]
                        require(page['KeyCount'] == len(entries) <= size, 'incorrect mixed page size')
                        actual.extend(sorted(entries))
                        if not page['IsTruncated']:
                            require('NextContinuationToken' not in page, 'token on final page')
                            break
                        token = page.get('NextContinuationToken')
                        require(token and token not in seen, 'missing or repeated continuation token')
                        seen.add(token)
                        require(len(seen) <= len(expected), 'pagination did not terminate')
                    require(actual == sorted(expected), f'pagination differs: {delim!r}, {size}, {actual}')
            return {'requests': requests, 'delimiter_variants': 4, 'page_sizes': [1, 3]}
        self.record('delimiter CommonPrefixes and mixed cross-directory pagination (required)', mixed_pages)

        def zero_and_start_after():
            zero = s3.list_objects_v2(Bucket=b, Prefix=prefix, Delimiter='/', MaxKeys=0)
            require(zero['KeyCount'] == zero['MaxKeys'] == 0, 'MaxKeys=0 changed to a nonzero limit')
            require(not zero['IsTruncated'] and not zero.get('Contents')
                    and not zero.get('CommonPrefixes'), 'MaxKeys=0 returned entries')
            marker = prefix + 'a/'
            first = s3.list_objects_v2(Bucket=b, Prefix=prefix, StartAfter=marker, MaxKeys=1)
            require(first['Contents'][0]['Key'] == prefix + 'a/deep/1', 'StartAfter differs')
            second = s3.list_objects_v2(Bucket=b, Prefix=prefix, StartAfter=prefix + 'z',
                                       ContinuationToken=first['NextContinuationToken'], MaxKeys=1)
            require(second['Contents'][0]['Key'] == prefix + 'a/deep/2', 'token must override StartAfter')
            grouped = s3.list_objects_v2(Bucket=b, Prefix=prefix, Delimiter='/', StartAfter=marker)
            require({'Prefix': marker} not in grouped.get('CommonPrefixes', []), 'StartAfter repeated group')
        self.record('zero MaxKeys and StartAfter/token precedence', zero_and_start_after)

        def invalid_parameters():
            first = s3.list_objects_v2(Bucket=b, Prefix=prefix, MaxKeys=1)
            token = first['NextContinuationToken']
            for args in ({'ContinuationToken': 'invalid-token'},
                         {'Prefix': prefix + 'a/', 'ContinuationToken': token},
                         {'Delimiter': '/', 'ContinuationToken': token},
                         {'EncodingType': 'invalid'}):
                options = dict(Bucket=b, Prefix=prefix)
                options.update(args)
                expect_error(lambda: s3.list_objects_v2(**options), 400)
            expect_error(lambda: s3.list_objects_v2(Bucket=b + '-missing', Prefix=prefix), 404)
        self.record('invalid listing parameters and missing bucket', invalid_parameters)

        def url_encoding():
            from urllib.parse import quote
            result = s3.list_objects_v2(Bucket=b, Prefix=prefix + '中文/', EncodingType='url')
            require(result['EncodingType'] == 'url', 'EncodingType was not echoed')
            expected = quote(prefix + '中文/空格+百分号%.txt', safe='')
            require([item['Key'] for item in result.get('Contents', [])] == [expected],
                    'explicit URL encoding differs')
        self.record('explicit encoding-type=url', url_encoding)

    def compatibility_probes(self):
        s3, b = self.client, self.bucket

        def defaults():
            client = self.make_client(compatible=False)
            data = b'default botocore checksum behavior'
            try:
                client.put_object(Bucket=b, Key='default-checksum.bin', Body=data)
                require(self.get('default-checksum.bin')[1] == data, 'default client content differs')
                return {'boto3': boto3.__version__, 'default_checksum': 'accepted over local HTTP'}
            finally:
                client.close()
        self.record('default boto3 checksum settings over HTTP', defaults, probe=True)

        def presigned():
            url = s3.generate_presigned_url('get_object', Params={'Bucket': b, 'Key': 'text.txt'}, ExpiresIn=60)
            # Do not put the signed URL in an exception/report.
            try:
                body = self.fetch(url)
            except Exception as error:
                status = getattr(error, 'code', 'unknown')
                raise AssertionError(f'presigned GET failed: HTTP {status} (URL omitted)') from None
            require(body == 'Curvine S3 end-to-end\n', 'presigned content differs')
        self.record('SigV4 presigned GET', presigned, probe=True)

    def stop(self):
        for key, upload in list(self.uploads):
            try:
                self.client.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload)
            except Exception:
                pass
        for name, process, log in reversed(self.processes):
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            log.close()
            (self.output / f'{name}.pid').unlink(missing_ok=True)
        if self.client:
            self.client.close()

    def report(self):
        return {'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                'started_at_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(self.started)),
                'boto3': boto3.__version__, 'botocore': botocore.__version__,
                'ports': self.ports, 'bucket': self.bucket,
                'topology': 'one Master, one Worker, standalone gateway, local part staging, one replica',
                'binary_versions': {name: subprocess.check_output(
                    [str(self.binary_dir / name), '--version'], text=True).strip()
                    for name in ('curvine-server', 'curvine-s3-gateway')},
                'binary_sha256': {name: sha256((self.binary_dir / name).read_bytes())
                                  for name in ('curvine-server', 'curvine-s3-gateway')},
                'cluster_stopped': all(p.poll() is not None for _, p, _ in self.processes),
                'results': self.results, 'compatibility_probes': self.probes,
                'summary': {status: sum(row['status'] == status for row in self.results) for status in ('PASS', 'FAIL')}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary-dir', type=Path, default=ROOT / 'target/release')
    parser.add_argument('--output-dir', type=Path, help='new directory; must not exist')
    parser.add_argument('--suite', choices=('full', 'listing'), default='full',
                        help='listing runs only required #2091 regressions; full also runs existing I/O tests')
    args = parser.parse_args()
    for binary in ('curvine-server', 'curvine-s3-gateway'):
        if not os.access(args.binary_dir / binary, os.X_OK):
            parser.error(f'missing executable: {args.binary_dir / binary}')
    if args.output_dir:
        args.output_dir.mkdir(parents=True, mode=0o700)
        output = args.output_dir.resolve()
    else:
        output = Path(tempfile.mkdtemp(prefix='curvine-s3-e2e-'))
    os.chmod(output, 0o700)
    suite = Suite(output, args.binary_dir.resolve())
    print(f'Artifacts: {output}', flush=True)
    try:
        if suite.record('start local cluster and create bucket', suite.start):
            if args.suite == 'full':
                suite.run_tests()
                suite.compatibility_probes()
            suite.listing_regressions()
    except Exception as error:
        suite.results.append({'name': 'suite execution', 'status': 'FAIL', 'error': str(error)})
        traceback.print_exc()
    finally:
        suite.stop()
        report = suite.report()
        report['test_scope'] = args.suite
        report['working_tree_status'] = subprocess.check_output(
            ['git', 'status', '--short'], cwd=ROOT, text=True).splitlines()
        report['gateway_source_sha256'] = {
            str(path.relative_to(ROOT)): sha256(path.read_bytes())
            for path in sorted((ROOT / 'curvine-s3-gateway/src').rglob('*.rs'))}
        report['runner_sha256'] = sha256(Path(__file__).read_bytes())
        (output / 'results.json').write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
        print(json.dumps(report['summary']), flush=True)
        print(f'Results: {output}/results.json; cluster stopped', flush=True)
    return 1 if report['summary']['FAIL'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
