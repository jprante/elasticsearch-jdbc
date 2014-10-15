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
package org.xbib.elasticsearch.action.plugin.jdbc.state.post;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PostRiverStateRequest extends AcknowledgedRequest<PostRiverStateRequest> {

    private String riverName;

    private String riverType;

    private RiverState riverState;

    private boolean abort;

    private boolean suspend;

    public PostRiverStateRequest setRiverName(String riverName) {
        this.riverName = riverName;
        return this;
    }

    public String getRiverName() {
        return riverName;
    }

    public PostRiverStateRequest setRiverType(String riverType) {
        this.riverType = riverType;
        return this;
    }

    public String getRiverType() {
        return riverType;
    }


    public PostRiverStateRequest setRiverState(RiverState riverState) {
        this.riverState = riverState;
        return this;
    }

    public RiverState getRiverState() {
        return riverState;
    }

    public PostRiverStateRequest setAbort() {
        this.abort = true;
        return this;
    }

    public boolean isAbort() {
        return abort;
    }

    public PostRiverStateRequest setSuspend() {
        this.suspend = true;
        return this;
    }

    public boolean isSuspend() {
        return suspend;
    }

    public PostRiverStateRequest setResume() {
        this.suspend = false;
        return this;
    }

    public boolean isResume() {
        return !suspend;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (riverName == null) {
            validationException = addValidationError("name is missing", null);
        }
        if (riverType == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readTimeout(in);
        this.riverName = in.readString();
        this.riverType = in.readString();
        if (in.readBoolean()) {
            this.riverState = new RiverState();
            riverState.readFrom(in);
        }
        this.abort = in.readBoolean();
        this.suspend = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeTimeout(out);
        out.writeString(riverName);
        out.writeString(riverType);
        if (riverState == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            riverState.writeTo(out);
        }
        out.writeBoolean(abort);
        out.writeBoolean(suspend);
    }
}