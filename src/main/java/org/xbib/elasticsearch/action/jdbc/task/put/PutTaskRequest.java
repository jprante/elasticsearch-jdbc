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
package org.xbib.elasticsearch.action.jdbc.task.put;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.xbib.elasticsearch.common.task.Task;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutTaskRequest extends AcknowledgedRequest<PutTaskRequest> {

    private String name;

    private Task task;

    public PutTaskRequest setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public PutTaskRequest setTask(Task task) {
        this.task = task;
        return this;
    }

    public Task getTask() {
        return task;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", null);
        }
        if (task == null) {
            validationException = addValidationError("task is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readTimeout(in);
        this.name = in.readString();
        if (in.readBoolean()) {
            this.task = new Task();
            task.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeTimeout(out);
        out.writeString(name);
        if (task == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            task.writeTo(out);
        }
    }
}