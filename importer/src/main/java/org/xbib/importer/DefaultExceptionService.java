package org.xbib.importer;

import org.xbib.importer.util.concurrent.ExceptionService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class DefaultExceptionService implements ExceptionService {

    private final Map<Runnable, Throwable> map = new ConcurrentHashMap<>();

    @Override
    public void setException(Runnable runnable, Throwable throwable) {
        map.put(runnable, throwable);
    }

    @Override
    public Map<Runnable, Throwable> getExceptions() {
        return map;
    }
}