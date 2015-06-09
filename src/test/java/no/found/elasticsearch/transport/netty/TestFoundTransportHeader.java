package no.found.elasticsearch.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;


public class TestFoundTransportHeader {
    @Test
    public void testHeaderBuffer() throws IOException {
        String clusterName = "test-cluster";
        String apiKey = "test-key";
        FoundTransportHeader fth = new FoundTransportHeader(clusterName, apiKey);

        ChannelBuffer headerBuffer = fth.getHeaderBuffer();

        assertEquals(headerBuffer.readInt(), headerBuffer.readableBytes()); // header length

        assertEquals(4, headerBuffer.readInt()); // revision size
        assertEquals(1, headerBuffer.readInt()); // revision

        assertEquals(8, headerBuffer.readInt()); // version size + found module version size
        assertEquals(Version.CURRENT.id, headerBuffer.readInt());
        assertEquals(FoundModuleVersion.CURRENT.id, headerBuffer.readInt());

        assertEquals(clusterName.length(), headerBuffer.readInt()); // cluster name size
        assertArrayEquals(clusterName.getBytes(StandardCharsets.UTF_8), headerBuffer.readBytes(clusterName.length()).array()); // cluster name

        assertEquals(apiKey.length(), headerBuffer.readInt()); // api key size
        assertArrayEquals(apiKey.getBytes(StandardCharsets.UTF_8), headerBuffer.readBytes(apiKey.length()).array()); // api key

        assertEquals(0, headerBuffer.readableBytes());
    }
}