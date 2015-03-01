package org.xbib.elasticsearch.common.task.worker;

import java.io.IOException;

public class WorkerException extends IOException {

    public WorkerException(String msg) {
        super(msg);
    }

    public WorkerException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
