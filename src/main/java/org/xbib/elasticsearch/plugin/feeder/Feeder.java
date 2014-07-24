package org.xbib.elasticsearch.plugin.feeder;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.xbib.elasticsearch.action.river.jdbc.state.RiverState;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.PipelineRequest;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * A feeder is a helper for executing feeds of documents to Elastisearch, either in push or pull mode.
 * Rivers use the pull mode, standalone programs operates usually in the the push mode.
 * The feed is executing by help of multiple threads with configurable concurrency.
 * Each thread starts a pipeline which iterates over the feed sources. Each pipeline
 * processes specifications that describe how the feed source is used.
 *
 * @param <T> the pipeline element
 * @param <R> the pipeline request
 * @param <P> the pipeline exception
 */
public interface Feeder<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        extends CommandLineInterpreter, Runnable, Closeable {

    /**
     * Set the type of the feeder
     *
     * @param type the type
     * @return the feeder
     */
    Feeder<T, R, P> setType(String type);

    /**
     * Set the specification for the feeder. The specification contains information
     * about a feeding step.
     *
     * @param spec the specification
     * @return the specification
     */
    Feeder<T, R, P> setSpec(Map<String, Object> spec);

    /**
     * Get the specification of this feeder
     *
     * @return the specification
     */
    Map<String, Object> getSpec();

    /**
     * Set settings for the feeder. The settings control the behaviour of the feeder.
     *
     * @param settings the settings
     * @return the feeder
     */
    Feeder<T, R, P> setSettings(Settings settings);

    /**
     * Get the feeder settings
     *
     * @return the settings
     */
    Settings getSettings();

    /**
     * Set the client for the feeder
     *
     * @param client the client
     * @return the feeder
     */
    Feeder<T, R, P> setClient(Client client);

    /**
     * Get the client of the feeder.
     *
     * @return the client
     */
    Client getClient();

    /**
     * Get the pipeline provider. The pipeline provider is called once per thread.
     *
     * @return the pipeline provider
     */
    PipelineProvider<P> pipelineProvider();

    /**
     * Prepare the feeder. This is the first phase of the feeder run. It is called before feeder threads
     * are started.
     *
     * @return the feeder
     * @throws java.io.IOException if prepare fails
     */
    Feeder<T, R, P> beforeRun() throws IOException;

    /**
     * Executing a task
     *
     * @param parameters the parameters for the task execution
     * @throws Exception if processing fails
     */
    void executeTask(Map<String, Object> parameters) throws Exception;

    /**
     * Clean up the feeder. This is the last phase of the feeder run. It is called after feeder threads
     * have ended.
     *
     * @return this feeder
     * @throws java.io.IOException if cleanup fails
     */
    Feeder<T, R, P> afterRun() throws IOException;

    /**
     * Set the state of the feeder if the feeder runs in a river.
     *
     * @param riverState the river state
     * @return the feeder
     */
    Feeder<T, R, P> setRiverState(RiverState riverState);

    /**
     * Get the feeder state if the feeder runs in a river.
     *
     * @return the river state
     */
    RiverState getRiverState();

    /**
     * Interrupt the feeder. The interruption request is delegated to all threads.
     *
     * @param state true if feeder should be interrupted
     */
    void setInterrupted(boolean state);

    /**
     * Returns if the feeder has been interrupted.
     *
     * @return true if interrupted
     */
    boolean isInterrupted();

}

