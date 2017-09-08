
package org.xbib.pipeline;

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * A pipeline is a Callable thread.
 * Pipelines share PipelineRequest by BlockingQueue.
 * Ex:
 *  1. star a importer and prepare settings
 *  2. wrap settings in PipelineRequestSettings and put in queue
 *  3. scheduled thread will call queue and poll settings
 *
 * @param <R> the pipeline request type
 */
public interface Pipeline<R extends PipelineRequest> extends Callable<R>, Closeable {

    Pipeline<R> setQueue(BlockingQueue<R> queue);

    BlockingQueue<R> getQueue();
}
