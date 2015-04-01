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
package org.xbib.elasticsearch.action.jdbc.task.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.xbib.elasticsearch.common.task.Task;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class GetTaskResponse extends ActionResponse implements ToXContent {

    private ImmutableList<Task> tasks = ImmutableList.of();

    public GetTaskResponse() {
    }

    public GetTaskResponse(ImmutableList<Task> tasks) {
        if (tasks != null) {
            this.tasks = tasks;
        }
    }

    public Task getNextTask() {
        if (tasks == null || tasks.isEmpty()) {
            return null;
        } else {
            return tasks.get(0);
        }
    }

    public ImmutableList<Task> getTasks() {
        return tasks;
    }

    public boolean exists(String name) {
        if (tasks != null && name != null) {
            for (Task task : tasks) {
                if (task != null && name.equals(task.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int len = in.readInt();
        ImmutableList.Builder<Task> builder = ImmutableList.builder();
        for (int i = 0; i < len; i++) {
            Task task = new Task();
            task.readFrom(in);
            builder.add(task);
        }
        tasks = builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (tasks == null) {
            out.writeInt(0);
        } else {
            out.writeInt(tasks.size());
            for (Task task : tasks) {
                task.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("task", tasks);
        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            builder = toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "";
        }
    }

}