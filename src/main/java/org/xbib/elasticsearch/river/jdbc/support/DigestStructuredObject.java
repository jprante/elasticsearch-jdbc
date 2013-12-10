
package org.xbib.elasticsearch.river.jdbc.support;

import org.elasticsearch.common.Base64;

import java.io.IOException;
import java.security.MessageDigest;

/**
 * A structured object with a digest.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class DigestStructuredObject extends PlainStructuredObject {

    private final static String DIGEST_ENCODING = "UTF-8";
    private MessageDigest digest;

    @Override
    public StructuredObject digest(MessageDigest digest) {
        this.digest = digest;
        return this;
    }

    @Override
    public MessageDigest digest() {
        return digest;
    }

    @Override
    public void checksum(String data) throws IOException {
        if (digest != null) {
            digest.update(data.getBytes(DIGEST_ENCODING));
        }
    }

    /**
     * Return a message digest (in base64-encoded form)
     *
     * @return the message digest
     */
    @Override
    public String checksum() {
        return digest != null ? Base64.encodeBytes(digest.digest()) : null;
    }
}
