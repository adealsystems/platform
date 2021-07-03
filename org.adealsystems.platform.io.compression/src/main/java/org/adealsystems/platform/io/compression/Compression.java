/*
 * Copyright 2020-2021 ADEAL Systems GmbH
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

package org.adealsystems.platform.io.compression;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public enum Compression {
    NONE,
    GZIP,
    BZIP;

    public static BufferedWriter createWriter(OutputStream outputStream, Compression compression)
        throws IOException {
        Objects.requireNonNull(outputStream, "outputStream must not be null!");
        Objects.requireNonNull(compression, "compression must not be null!");
        switch (compression) {
            case NONE:
                return new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            case GZIP:
                return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(outputStream), StandardCharsets.UTF_8));
            case BZIP:
                return new BufferedWriter(new OutputStreamWriter(new BZip2CompressorOutputStream(outputStream), StandardCharsets.UTF_8));
            default:
                // unreachable - unless we fail while adding more compressions
                throw new IllegalArgumentException("Compression " + compression + " isn't (yet) supported!");
        }
    }

    public static BufferedReader createReader(InputStream inputStream, Compression compression)
        throws IOException {
        Objects.requireNonNull(inputStream, "inputStream must not be null!");
        Objects.requireNonNull(compression, "compression must not be null!");
        switch (compression) {
            case NONE:
                return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            case GZIP:
                return new BufferedReader(new InputStreamReader(new GZIPInputStream(inputStream), StandardCharsets.UTF_8));
            case BZIP:
                return new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(inputStream), StandardCharsets.UTF_8));
            default:
                // unreachable - unless we fail while adding more compressions
                throw new IllegalArgumentException("Compression " + compression + " isn't (yet) supported!");
        }
    }
}
