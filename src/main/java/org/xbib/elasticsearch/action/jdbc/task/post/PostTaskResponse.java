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

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class PostTaskResponse extends NodesOperationResponse<PostTaskResponse.NodeTaskResponse> implements ToXContent {

    public PostTaskResponse() {
    }

    public PostTaskResponse(ClusterName clusterName, NodeTaskResponse[] nodes) {
        super(clusterName, nodes);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(nodes.length);
        for (NodeTaskResponse node : nodes) {

        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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

    public static class NodeTaskResponse extends NodeOperationResponse {
        NodeTaskResponse() {

        }

        public NodeTaskResponse(DiscoveryNode node) {
            super(node);
        }

        public static NodeTaskResponse readNodeTaskResponse(StreamInput in) throws IOException {
            NodeTaskResponse res = new NodeTaskResponse();
            res.readFrom(in);
            return res;
        }
    }

}