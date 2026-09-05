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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CurvineJavaUtils {
    private static final Pattern BYTE_SIZE_PATTERN =
            Pattern.compile("^([+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+))\\s*([a-zA-Z]*)$");

    private CurvineJavaUtils() {}

    public static String bytesToString(long size) {
        return bytesToString(BigIntegerCompat.fromLong(size));
    }

    public static String bytesToString(BigIntegerCompat size) {
        final long eib = 1L << 60;
        final long pib = 1L << 50;
        final long tib = 1L << 40;
        final long gib = 1L << 30;
        final long mib = 1L << 20;
        final long kib = 1L << 10;

        if (size.compareTo(BigIntegerCompat.fromLong(1L << 11).multiply(eib)) >= 0) {
            return new BigDecimal(size.toString(), new MathContext(3, RoundingMode.HALF_UP)) + " B";
        }

        BigDecimal value;
        String unit;
        if (size.compareTo(BigIntegerCompat.fromLong(2L * eib)) >= 0) {
            value = new BigDecimal(size.toString()).divide(BigDecimal.valueOf(eib), MathContext.DECIMAL64);
            unit = "EB";
        } else if (size.compareTo(BigIntegerCompat.fromLong(2L * pib)) >= 0) {
            value = new BigDecimal(size.toString()).divide(BigDecimal.valueOf(pib), MathContext.DECIMAL64);
            unit = "PB";
        } else if (size.compareTo(BigIntegerCompat.fromLong(2L * tib)) >= 0) {
            value = new BigDecimal(size.toString()).divide(BigDecimal.valueOf(tib), MathContext.DECIMAL64);
            unit = "TB";
        } else if (size.compareTo(BigIntegerCompat.fromLong(2L * gib)) >= 0) {
            value = new BigDecimal(size.toString()).divide(BigDecimal.valueOf(gib), MathContext.DECIMAL64);
            unit = "GB";
        } else if (size.compareTo(BigIntegerCompat.fromLong(2L * mib)) >= 0) {
            value = new BigDecimal(size.toString()).divide(BigDecimal.valueOf(mib), MathContext.DECIMAL64);
            unit = "MB";
        } else if (size.compareTo(BigIntegerCompat.fromLong(2L * kib)) >= 0) {
            value = new BigDecimal(size.toString()).divide(BigDecimal.valueOf(kib), MathContext.DECIMAL64);
            unit = "KB";
        } else {
            value = new BigDecimal(size.toString());
            unit = "B";
        }

        return String.format(Locale.US, "%.1f%s", value, unit);
    }

    public static long byteFromString(String str) {
        if (str == null) {
            throw new IllegalArgumentException("Size string must not be null");
        }

        Matcher matcher = BYTE_SIZE_PATTERN.matcher(str.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid size string: " + str);
        }

        BigDecimal value = new BigDecimal(matcher.group(1));
        if (value.signum() < 0) {
            throw new IllegalArgumentException("Size string must not be negative: " + str);
        }

        BigDecimal bytes = value.multiply(BigDecimal.valueOf(byteUnitMultiplier(matcher.group(2))));
        try {
            return bytes.setScale(0, RoundingMode.HALF_UP).longValueExact();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Size string is out of long range: " + str, e);
        }
    }

    private static long byteUnitMultiplier(String unit) {
        String normalized = unit == null ? "" : unit.trim().toUpperCase(Locale.ROOT);
        switch (normalized) {
            case "":
            case "B":
            case "BYTE":
            case "BYTES":
                return 1L;
            case "K":
            case "KB":
            case "KIB":
                return 1L << 10;
            case "M":
            case "MB":
            case "MIB":
                return 1L << 20;
            case "G":
            case "GB":
            case "GIB":
                return 1L << 30;
            case "T":
            case "TB":
            case "TIB":
                return 1L << 40;
            case "P":
            case "PB":
            case "PIB":
                return 1L << 50;
            case "E":
            case "EB":
            case "EIB":
                return 1L << 60;
            default:
                throw new IllegalArgumentException("Unsupported size unit: " + unit);
        }
    }

    public static Configuration getCurvineConf() {
        String confDir = System.getProperty("curvine.conf.dir");
        Configuration conf = new Configuration(false);
        File file = new File(Paths.get(confDir, "curvine-site.xml").toString());
        conf.addResource(new Path(file.getPath()));

        if (conf.get("fs.cv.impl") == null) {
            conf.set("fs.cv.impl", "io.curvine.CurvineFileSystem");
        }

        if (conf.get("fs.AbstractFileSystem.cv.impl") == null) {
            conf.set("fs.AbstractFileSystem.cv.impl", "io.curvine.CurvineAbstractFileSystem");
        }

        return conf;
    }

    /**
     * Small immutable wrapper to keep the utility Java-only and avoid pulling
     * scala-library into runtime jars for size formatting helpers.
     */
    public static final class BigIntegerCompat {
        private final java.math.BigInteger value;

        private BigIntegerCompat(java.math.BigInteger value) {
            this.value = value;
        }

        public static BigIntegerCompat fromLong(long value) {
            return new BigIntegerCompat(java.math.BigInteger.valueOf(value));
        }

        public BigIntegerCompat multiply(long value) {
            return new BigIntegerCompat(this.value.multiply(java.math.BigInteger.valueOf(value)));
        }

        public int compareTo(BigIntegerCompat other) {
            return this.value.compareTo(other.value);
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }
}
