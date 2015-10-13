
package org.xbib.pipeline;

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * A pipeline.
 *
 * @param <R> the pipeline request type
 */
public interface Pipeline<R extends PipelineRequest> extends Callable<R>, Closeable {

    Pipeline<R> setQueue(BlockingQueue<R> queue);

    BlockingQueue<R> getQueue();
}
