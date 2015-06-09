/*
 * This is the MIT license: http://www.opensource.org/licenses/mit-license.php
 *
 * Copyright (c) 2010-2012, Found IT A/S.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
 * to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
 * FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
package no.found.elasticsearch.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBuffers;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * An utility class for generating the initial headers for auth.
 */
public class FoundTransportHeader {
    private final String clusterName;
    private final String apiKey;

    private static final int revisionLength = 4;
    private static final int revision = 1;

    private static final int versionLength = 4;
    private static final int moduleVersionLength = 4;

    public FoundTransportHeader(String clusterName, String apiKey) {
        this.clusterName = clusterName;
        this.apiKey = apiKey;
    }

    /**
     * Constructs and returns a new ChannelBuffer with the correct header for the given
     * cluster and API-key.
     *
     * @return The ChannelBuffer containing the header.
     * @throws java.io.IOException
     */
    public ChannelBuffer getHeaderBuffer() throws IOException {
        byte[] clusterNameBytes = clusterName.getBytes(StandardCharsets.UTF_8);
        int clusterNameLength = clusterNameBytes.length;

        byte[] apiKeyBytes = apiKey.getBytes(StandardCharsets.UTF_8);
        int apiKeyLength = apiKeyBytes.length;

        ChannelBuffer headerPayload = ChannelBuffers.wrappedBuffer(
                getIntBytes(revisionLength),
                getIntBytes(revision),
                getIntBytes(versionLength + moduleVersionLength),
                getIntBytes(Version.CURRENT.id),
                getIntBytes(FoundModuleVersion.CURRENT.id),
                getIntBytes(clusterNameLength),
                clusterNameBytes,
                getIntBytes(apiKeyLength),
                apiKeyBytes
        );

        return ChannelBuffers.wrappedBuffer(
                ChannelBuffers.wrappedBuffer(getIntBytes(headerPayload.readableBytes())),
                headerPayload
        );
    }

    protected byte[] getIntBytes(int i) throws IOException {
        byte[] bytes = new byte[4];

        bytes[0] = ((byte) (i >> 24));
        bytes[1] = ((byte) (i >> 16));
        bytes[2] = ((byte) (i >> 8));
        bytes[3] = ((byte) i);

        return bytes;
    }
}