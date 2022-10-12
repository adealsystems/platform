/*
 * Copyright 2020-2022 ADEAL Systems GmbH
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

package org.adealsystems.platform.io.test;

import java.io.IOException;
import java.io.InputStream;

public class BrokenInputStream extends InputStream {
    private boolean broken;
    private final InputStream backingStream;

    public BrokenInputStream(InputStream backingStream) {
        this.backingStream = backingStream;
    }

    @Override
    public int read() throws IOException {
        if (broken) {
            throw new BrokenStreamException();
        }
        return backingStream.read();
    }

    @Override
    public void close() throws IOException {
        super.close();
        backingStream.close();
        if (broken) {
            throw new BrokenStreamException();
        }
    }

    public void setBroken(boolean broken) {
        this.broken = broken;
    }
}
