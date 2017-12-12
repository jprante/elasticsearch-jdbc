package org.xbib.jdbc.input;

/**
 * Created by sanyu on 2017/9/7.
 */
public interface Input {

    String ROOT_PATH = "/input_init";

    /**
     * 1. load settings
     */
    void init();

    /**
     * 2. prepare job
     */
    void prepare();

    /**
     * 3. run thread
     */
    void execute();
}
