/*
 * Copyright 2020-2025 ADEAL Systems GmbH
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

package org.adealsystems.platform.orchestrator.executor.email;

import java.util.Objects;

public class EmailAttachment {

    public final String filename;
    public final boolean attachmentAvailable;
    public final String attachmentName;
    public final String emptyMaxSize;
    public final String title;

    public EmailAttachment(String filename, boolean attachmentAvailable, String attachmentName, String emptyMaxSize, String title) {
        this.filename = Objects.requireNonNull(filename, "filename must not be null!");
        this.attachmentAvailable = attachmentAvailable;
        this.attachmentName = attachmentName;
        this.emptyMaxSize = emptyMaxSize;
        this.title = Objects.requireNonNull(title, "title must not be null!");
    }

    public String getFilename() {
        return filename;
    }

    public boolean isAttachmentAvailable() {
        return attachmentAvailable;
    }

    public String getAttachmentName() {
        return attachmentName;
    }

    public String getEmptyMaxSize() {
        return emptyMaxSize;
    }

    public String getTitle() {
        return title;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EmailAttachment)) return false;
        EmailAttachment that = (EmailAttachment) o;
        return attachmentAvailable == that.attachmentAvailable
            && Objects.equals(filename, that.filename)
            && Objects.equals(attachmentName, that.attachmentName)
            && Objects.equals(emptyMaxSize, that.emptyMaxSize)
            && Objects.equals(title, that.title);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, attachmentAvailable, attachmentName, emptyMaxSize, title);
    }

    @Override
    public String toString() {
        return "EmailAttachment{" +
            "filename='" + filename + '\'' +
            ", attachmentAvailable=" + attachmentAvailable +
            ", attachmentName='" + attachmentName + '\'' +
            ", emptyMaxSize='" + emptyMaxSize + '\'' +
            ", title='" + title + '\'' +
            '}';
    }
}
