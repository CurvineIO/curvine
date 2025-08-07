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

package io.curvine;

import io.curvine.bench.Utils;
import io.curvine.proto.MountPointInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.junit.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class CurvineFileSystemTest {
    static FileSystem fs;

    @BeforeClass
    public static void before() throws Exception {
        String useDir = System.getProperty("user.dir");
        System.setProperty("java.library.path", useDir + "/../../target/debug");
        System.setProperty("curvine.conf.dir", useDir + "/../../etc");
        Configuration conf = Utils.getCurvineConf();
        conf.set("fs.cv.master_addrs", "localhost:52389");

        fs = FileSystem.get(conf);
    }

    @AfterClass
    public static void after() throws Exception {
        if (fs != null) {
            fs.close();
        }
    }

    @Test
    public void getMountInfo() throws Exception {
        Path path = new Path("s3://flink/xuen-test");
        MountPointInfo mountPoint = ((CurvineFileSystem) fs).getMountPoint(path);
        System.out.println("mount point: " + mountPoint);
    }

    @Test
    public void base_test() throws Exception {
        mkdirs();
        writeAndRead();
        seek();
        rename();
        delete();
        getFileStatus();
        listStatus();
        listFiles();
        hasError();
    }

    public void mkdirs() throws IOException {
        Path path = new Path("/a/b");
        boolean ret = fs.mkdirs(path);

        FileStatus status = fs.getFileStatus(path);
        System.out.println("dir status: " + status);
        assert status.isDirectory();
    }

    public void writeAndRead() throws IOException {
        Path path = new Path("/a/b/aaa.txt");

        int write_checksum = 0;
        byte[] buf = StringUtils.repeat("A", 64 * 1024).getBytes(StandardCharsets.UTF_8);
        try (FSDataOutputStream os = fs.create(path, true)) {
            int count = 0;
            while (count < 200 * 1024 * 1024) {
                os.write(buf);
                count += buf.length;
                write_checksum += Arrays.hashCode(buf);
            }
        }

        byte[] read_buf = new byte[64 * 1024];
        int read_checksum = 0;
        try (FSDataInputStream in = fs.open(path)) {
            while (in.read(read_buf) != -1) {
                read_checksum += Arrays.hashCode(read_buf);
            }
        }
        System.out.println("write checksum " + write_checksum);
        System.out.println("read checksum " + read_checksum);

        assert write_checksum == read_checksum;
    }

    public void seek() throws IOException {
        Path path = new Path("/a/b/aaa.txt");
        byte[] buf = new byte[100];
        long pos = 50;
        try (FSDataInputStream in = fs.open(path)) {
            in.read(buf);
            in.seek(pos);
            int size = in.read(buf);
            assert pos + size == in.getPos();
        }
    }

    public void rename() throws IOException {
        Path src = new Path("/a/b/aaa.txt");
        Path dst = new Path("/a/b/bbb.txt");
        boolean ret = fs.rename(src, dst);
        System.out.println("ret: " + ret);
        if (ret) {
            FileStatus stat = fs.getFileStatus(dst);
            System.out.println(stat);
        }
    }

    public void delete() throws IOException {
        Path path = new Path("/a/b/bbb.txt");
        boolean ret = fs.delete(path, true);
        System.out.println("ret: " + ret);
    }

    public void getFileStatus() throws IOException {
        Path path = new Path("/a/b");
        FileStatus stat = fs.getFileStatus(path);
        System.out.println(stat);
    }

    public void listStatus() throws IOException {
        fs.mkdirs(new Path("/list-status"));
        Path path1 = new Path("/list-status/1.log");
        Path path2 = new Path("/list-status/2.log");
        fs.create(path1, true).close();
        fs.create(path2, true).close();

        fs.mkdirs(new Path("/list-status/dir"));

        FileStatus[] stat = fs.listStatus(new Path("/list-status"));
        for (FileStatus item : stat) {
            System.out.println(item);
        }

        assert stat.length == 3;
    }

    public void listFiles() throws IOException {
        fs.mkdirs(new Path("/list-files"));
        Path path1 = new Path("/list-files/1.log");
        Path path2 = new Path("/list-files/2.log");
        fs.create(path1, true).close();
        fs.create(path2, true).close();

        fs.mkdirs(new Path("/list-files/dir"));

        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/list-files"), true);
        ArrayList<FileStatus> list = new ArrayList<>();
       while (iter.hasNext()) {
           FileStatus status = iter.next();
           System.out.println(status);
           list.add(status);
       }

        assert list.size() == 2;
        assert list.get(0).isFile();
        assert list.get(1).isFile();
    }

    public void hasError() {
        String errorMsg = "";
        try {
            fs.create(new Path("/has-error.log"), false).close();
            fs.create(new Path("/has-error.log"), false);
        } catch (Exception e) {
            e.printStackTrace();
            errorMsg = e.getMessage();
        }

        assert errorMsg.contains("already exists");
    }

    @Test
    public void append() throws Exception {
        Path path = new Path("/append.txt");
        FSDataOutputStream writer = fs.create(path);
        writer.write("123".getBytes(StandardCharsets.UTF_8));
        writer.close();

        FSDataOutputStream writer1 = fs.append(path);
        assert writer1.getPos() == 3;

        writer1.write("abc".getBytes(StandardCharsets.UTF_8));
        writer1.close();

        FSDataInputStream reader = fs.open(path);
        byte[] bytes = new byte[6];
        reader.read(bytes);
        assert new String(bytes).equals("123abc");
        assert reader.getPos() == 6;
    }
}