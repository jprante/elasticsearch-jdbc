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
package org.xbib.elasticsearch.common.task.cluster;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elasticsearch.common.task.Task;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

/**
 * Contains metadata about tasks
 */
public class ClusterTaskMetaData implements MetaData.Custom {

    public static final String TYPE = "tasks";

    public static final Factory FACTORY = new Factory();

    private final ImmutableList<Task> tasks;

    /**
     * Constructs new task metadata
     *
     * @param tasks list of tasks
     */
    public ClusterTaskMetaData(Task... tasks) {
        this.tasks = ImmutableList.copyOf(tasks);
    }

    /**
     * Returns list of current tasks
     *
     * @return list of tasks
     */
    public ImmutableList<Task> getTasks() {
        return this.tasks;
    }

    /**
     * Returns a task with a given name or null if such doesn't exist
     *
     * @param name name
     * @return metadata
     */
    public ImmutableList<Task> getTask(String name) {
        ImmutableList.Builder<Task> taskBuilder = ImmutableList.builder();
        for (Task task : tasks) {
            if (("*".equals(name) || name.equals(task.getName()))) {
                taskBuilder.add(task);
            }
        }
        return taskBuilder.build();
    }

    /**
     * Task metadata factory
     */
    public static class Factory extends MetaData.Custom.Factory<ClusterTaskMetaData> {

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public ClusterTaskMetaData readFrom(StreamInput in) throws IOException {
            Task[] tasks = new Task[in.readVInt()];
            for (int i = 0; i < tasks.length; i++) {
                tasks[i] = new Task();
                tasks[i].readFrom(in);
            }
            return new ClusterTaskMetaData(tasks);
        }

        @Override
        public void writeTo(ClusterTaskMetaData stateMetaData, StreamOutput out) throws IOException {
            out.writeVInt(stateMetaData.getTasks().size());
            for (Task state : stateMetaData.getTasks()) {
                state.writeTo(out);
            }
        }

        @Override
        public ClusterTaskMetaData fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            List<Task> stateList = newLinkedList();
            if (token == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                    throw new ElasticsearchParseException("failed to parse task at [" + name + "], expected array");
                }
            }
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                Task task = new Task();
                task.fromXContent(parser);
                stateList.add(task);
            }
            return new ClusterTaskMetaData(stateList.toArray(new Task[stateList.size()]));
        }

        @Override
        public void toXContent(ClusterTaskMetaData stateMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startArray("states");
            for (Task tasks : stateMetaData.getTasks()) {
                tasks.toXContent(builder, params);
            }
            builder.endArray();
        }

    }

}