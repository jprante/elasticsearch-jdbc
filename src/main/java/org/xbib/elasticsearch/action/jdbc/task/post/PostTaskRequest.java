/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.action.jdbc.task.post;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.NodesOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.xbib.elasticsearch.common.task.Task;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PostTaskRequest extends NodesOperationRequest<PostTaskRequest> {

    private String name;

    private Task task;

    private boolean abort;

    private boolean suspend;

    public PostTaskRequest setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public PostTaskRequest setTask(Task task) {
        this.task = task;
        return this;
    }

    public Task getTask() {
        return task;
    }

    public PostTaskRequest setAbort() {
        this.abort = true;
        return this;
    }

    public boolean isAbort() {
        return abort;
    }

    public PostTaskRequest setSuspend() {
        this.suspend = true;
        return this;
    }

    public boolean isSuspend() {
        return suspend;
    }

    public PostTaskRequest setResume() {
        this.suspend = false;
        return this;
    }

    public boolean isResume() {
        return !suspend;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", null);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.name = in.readString();
        if (in.readBoolean()) {
            this.task = new Task();
            task.readFrom(in);
        }
        this.abort = in.readBoolean();
        this.suspend = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        if (task == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            task.writeTo(out);
        }
        out.writeBoolean(abort);
        out.writeBoolean(suspend);
    }
}