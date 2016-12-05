package org.xbib.importer.util.concurrent;

import java.util.Map;

/**
 */
public interface ExceptionService {

    void setException(Runnable runnable, Throwable throwable);

    Map<Runnable, Throwable> getExceptions();
}
