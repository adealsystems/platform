/*
 * Copyright 2020-2023 ADEAL Systems GmbH
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

package org.adealsystems.platform.orchestrator.executor;

public class ExecutorFileSizeResponse {
    private final ExecutorExitCode exitCode;
    private final Long size;

    public ExecutorFileSizeResponse(ExecutorExitCode exitCode, Long size){
        this.exitCode = exitCode;
        this.size = size;
    }
    public ExecutorFileSizeResponse(ExecutorExitCode exitCode){
        this.exitCode = exitCode;
        this.size = null;
    }
    public ExecutorExitCode getExitCode() {
        return exitCode;
    }

    public Long getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "ExecutorFileSizeResponse{" +
            "exitCode=" + exitCode +
            ", size=" + size +
            '}';
    }
}
