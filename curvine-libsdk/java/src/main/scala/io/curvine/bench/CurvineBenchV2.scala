/*
 * Copyright 2025 OPPO.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.curvine.bench
import io.curvine.bench.CurvineBenchV2.LOGGER
import io.curvine.executor.FixedAllocationExecutor
import org.apache.commons.lang3.RandomStringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import java.io._
import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable.ListBuffer

// v2 version performance test code.
class CurvineBenchV2(params: BenchParams) {
  private val dir = params.dataDir

  private val executor = new FixedAllocationExecutor(params.threads, 100, 0);

  private val checksum = new LongAdder

  private val totalLen = new LongAdder

  private val loopNum: Int = (params.fileSize / params.bufferSize).toInt

  private lazy val fs: FileSystem = {
    val uri = new Path(s"cv://$dir").toUri
    FileSystem.get(uri, Utils.getCurvineConf)
  }

  val (isFuse, action) = parseAction()

  checkDir()

  private def checkDir(): Unit = {
    if (isFuse) {
      new File(dir).mkdirs()
    } else {
      fs.mkdirs(new Path(dir))
    }
  }

  def parseAction(): (Boolean, String) = {
    val arr = params.action.split("\\.")
    if (arr.length != 2) {
      throw new RuntimeException("param error")
    }
    if (arr(0) == "fuse") {
      (true, arr(1))
    } else {
      (false, arr(1))
    }
  }

  private def createReader(index: Int): DataInputStream = {
    if (isFuse) {
      // use local file
      val file = new File(dir, index + "")
      if (params.useNIO) {
        new DataInputStream(new NIOFileInputStream(file, params.bufferSize))
      } else {
        new DataInputStream(new FileInputStream(file))
      }
    } else {
      // use hadoop file
      val path = new Path(dir, index + "")
      fs.open(path)
    }
  }

  private def createWriter(index: Int): DataOutputStream = {
    if (isFuse) {
      // use local file
      val file = new File(dir, index + "")
      if (params.useNIO) {
        new DataOutputStream(new NIOFileOutputStream(file, params.bufferSize))
      } else {
        new DataOutputStream(new FileOutputStream(file))
      }
    } else {
      // use hadoop file
      val path = new Path(dir, index + "")
      fs.create(path, true)
    }
  }

  def createAllReader(): ListBuffer[(DataInputStream, Array[Byte])] = {
    val list = ListBuffer[(DataInputStream, Array[Byte])]()
    0.until(params.fileNum).foreach(index => {
      val start = System.currentTimeMillis()
      list.append((createReader(index), new Array[Byte](params.bufferSize)))
      LOGGER.info(s"create reader $index cost ${System.currentTimeMillis() - start} ms")
    })

    list
  }

  def createAllWriter(): ListBuffer[(DataOutputStream, Array[Array[Byte]])] = {
    val list = ListBuffer[(DataOutputStream, Array[Array[Byte]])]()
    0.until(params.fileNum).foreach(index => {
      val start = System.currentTimeMillis()
      list.append((createWriter(index), create_write_bytes()))
      LOGGER.info(s"create writer $index cost ${System.currentTimeMillis() - start} ms")
    })

    list
  }

  def getChecksum: Long = {
    checksum.longValue()
  }

  def getTotalLenString: String = {
    Utils.bytesToString(totalLen.longValue())
  }

  def create_write_bytes(): Array[Array[Byte]] = {
    val bytes = new Array[Array[Byte]](3)
    0.until(3).foreach { i =>
      val original = RandomStringUtils.randomAscii(params.bufferSize)
      val modified = original.substring(0, original.length - 1) + "\n"
      bytes(i) = modified.getBytes
    }

    bytes
  }

  def runWrite(writers: ListBuffer[(DataOutputStream, Array[Array[Byte]])]): Unit = {
    0.until(loopNum).foreach(i => {
      val bytesIndex = i % 3
      0.until(params.fileNum).foreach(index => {
        executor.execute(index, () => {
          val (writer, rand_bytes) = writers(index)
          writer.write(rand_bytes(bytesIndex))

          if (params.checksum) {
            totalLen.add(rand_bytes(bytesIndex).length)
            checksum.add(Utils.crc32(rand_bytes(bytesIndex)))
          }

          if (i == loopNum - 1) {
            writer.close()
          }
        })
      })
    })

    executor.shutdown()
  }

  def runRead(readers: ListBuffer[(DataInputStream, Array[Byte])]): Unit = {
    0.until(loopNum).foreach(i => {
      0.until(params.fileNum).foreach(index => {
        executor.execute(index, () => {
          val (reader, bytes) = readers(index)
          reader.readFully(bytes)

          if (params.checksum) {
            totalLen.add(bytes.length)
            checksum.add(Utils.crc32(bytes))
          }

          if (i == loopNum - 1) {
            reader.close()
          }
        })
      })
    })

    executor.shutdown()
  }
}

object CurvineBenchV2 {
  val LOGGER: Logger = LoggerFactory.getLogger(CurvineBenchV2.getClass)

  def main(args: Array[String]): Unit = {
    val params = BenchParams.buildFromArgs(args)
    val bench = new CurvineBenchV2(params)

    var start: Long = 0
    bench.action match {
      case "write" =>
        val writes = bench.createAllWriter()
        start = System.currentTimeMillis()

        LOGGER.info(s"${params.action} bench start")
        bench.runWrite(writes)

      case "read" =>
        val readers = bench.createAllReader()
        start = System.currentTimeMillis()

        LOGGER.info(s"${params.action} bench start")
        bench.runRead(readers)

      case _ => throw new RuntimeException(s"Unsupported operations ${params.action}")
    }

    val cost = (System.currentTimeMillis() - start) / 1000D
    val base = (params.totalSize() / cost).toLong

    val spreed = Utils.bytesToString(base)
    val width =  Utils.bytesToString(base * 8)

    LOGGER.info(s"${params.action} bench end\n"
      + s"size: ${bench.getTotalLenString}, cost: $cost s, speed: $spreed/s, width: $width/s\n"
      + s"checksum: ${bench.getChecksum}\nparams: $params")
  }

}