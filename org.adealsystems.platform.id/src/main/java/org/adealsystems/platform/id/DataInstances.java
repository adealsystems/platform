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

package org.adealsystems.platform.id;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Objects;

public final class DataInstances {
    private static final String SUCCESS_FILE = "_SUCCESS";

    private DataInstances() {
    }

    public static PrintWriter createWriter(DataInstance dataInstance) throws IOException {
        return new PrintWriter(new OutputStreamWriter(dataInstance.getOutputStream(), StandardCharsets.UTF_8));
    }

    public static BufferedReader createReader(DataInstance dataInstance) throws IOException {
        return new BufferedReader(new InputStreamReader(dataInstance.getInputStream(), StandardCharsets.UTF_8));
    }

    public static BufferedReader createBatchReader(DataInstance dataInstance) throws IOException {
        Objects.requireNonNull(dataInstance, "dataInstance must not be null!");
        return new BufferedReader(new InputStreamReader(createBatchInputStream(dataInstance), StandardCharsets.UTF_8));
    }

    public static InputStream createBatchInputStream(DataInstance dataInstance)
            throws IOException {
        Objects.requireNonNull(dataInstance, "dataInstance must not be null!");
        String path = dataInstance.getPath();

        File f = new File(path);
        if (!f.isDirectory()) {
            throw new IllegalArgumentException("Path '" + path + "' isn't a directory!");
        }
        File[] files = f.listFiles();
        if (files == null) {
            throw new IllegalStateException("Directory '" + path + "' didn't contain any files!");
        }

        String extension = dataInstance.getDataFormat().getExtension();

        boolean success = false;
        int counter = 0;
        File dataFile = null;
        for (File file : files) {
            String fileName = file.getName();
            if (SUCCESS_FILE.equals(fileName)) {
                success = true;
                continue;
            }
            if (fileName.endsWith(extension)) {
                counter++;
                dataFile = file;
            }
        }

        if (!success) {
            throw new IllegalStateException("Could not find " + SUCCESS_FILE + "!");
        }
        if (counter != 1) {
            throw new IllegalStateException("Found " + counter + " candidate files instead of 1!");
        }

        return Files.newInputStream(dataFile.toPath());
    }
}
