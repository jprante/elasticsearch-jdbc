
package org.xbib.elasticsearch.river.jdbc.support;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.List;

/**
 * Value listener interface for receiving a stream of key/value attributes,
 * like a number of rows of columns.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public interface ValueListener {

    /**
     * Set the key names for the values
     *
     * @param keys the keys
     * @return this ValueListener
     */
    ValueListener keys(List<String> keys);

    /**
     * Receive values for the keys
     *
     * @param values the values
     * @return this ValueListener
     * @throws IOException
     */
    ValueListener values(List<? extends Object> values) throws IOException;

    /**
     * Reset the key/value configuration
     *
     * @return this value listener
     * @throws IOException
     */
    ValueListener reset() throws IOException;

    /**
     * Compute digest value for these values.
     *
     * @return digest
     */
    MessageDigest digest();
}
