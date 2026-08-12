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

import org.apache.hadoop.conf.{Configuration, StorageSize}
import org.apache.hadoop.fs.Path

import java.io.File
import java.math.{MathContext, RoundingMode}
import java.nio.file.Paths
import java.util.Locale
import java.util.zip.CRC32

object Utils {
  def bytesToString(size: BigInt): String = {
    val EiB = 1L << 60
    val PiB = 1L << 50
    val TiB = 1L << 40
    val GiB = 1L << 30
    val MiB = 1L << 20
    val KiB = 1L << 10

    if (size >= BigInt(1L << 11) * EiB) {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    } else {
      val (value, unit) = {
        if (size >= 2 * EiB) {
          (BigDecimal(size) / EiB, "EB")
        } else if (size >= 2 * PiB) {
          (BigDecimal(size) / PiB, "PB")
        } else if (size >= 2 * TiB) {
          (BigDecimal(size) / TiB, "TB")
        } else if (size >= 2 * GiB) {
          (BigDecimal(size) / GiB, "GB")
        } else if (size >= 2 * MiB) {
          (BigDecimal(size) / MiB, "MB")
        } else if (size >= 2 * KiB) {
          (BigDecimal(size) / KiB, "KB")
        } else {
          (BigDecimal(size), "B")
        }
      }
      "%.1f%s".formatLocal(Locale.US, value, unit)
    }
  }

  def bytesToString(size: Long): String = {
    bytesToString(BigInt(size))
  }

  def byteFromString(str: String): Long = {
    val size = StorageSize.parse(str)
    size.getUnit.toBytes(size.getValue).toLong
  }

  def getCurvineConf: Configuration = {
    val confDir = System.getProperty("curvine.conf.dir")
    val conf = new Configuration(false)
    val file = new File(Paths.get(confDir, "curvine-site.xml").toString)
    conf.addResource(new Path(file.getPath))

    if (conf.get("fs.curvine.impl") == null) {
      conf.set("fs.curvine.impl", "io.curvine.CurvineFileSystem")
    }

    conf
  }

  def crc32(bytes: Array[Byte]): Long  = {
    val crc32 = new CRC32()
    crc32.update(bytes, 0, bytes.length)
    crc32.getValue
  }
}
