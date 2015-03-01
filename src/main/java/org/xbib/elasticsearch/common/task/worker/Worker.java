package org.xbib.elasticsearch.common.task.worker;

import org.elasticsearch.plugins.Plugin;
import org.xbib.elasticsearch.common.task.worker.job.Job;
import org.xbib.elasticsearch.common.task.worker.job.JobExecutionListener;

import java.io.IOException;

public interface Worker extends Plugin {

    /**
     * The name of this Worker
     * @return the name of the worker
     */
    String name();

    /**
     * Perform asynchronous execution of a job
     * @param job the job
     * @param listener a job execution listener with the result of the execution
     * @throws WorkerException if execution fails
     */
    void execute(Job job, JobExecutionListener listener) throws WorkerException;

    /**
     * Get number of pending jobs
     * @return the number of pending jobs
     */
    int getPendingJobs();

    /**
     * Wait for all pending jobs to terminate.
     * @throws WorkerException if wait fails
     */
    void waitForPendingJobs() throws WorkerException;

    /**
     * Suspend this worker. All current jobs are halted.
     * @throws WorkerException if suspend fails
     */
    void suspend() throws WorkerException;

    /**
     * Resume this worker. Halted jobs are restarted.
     * @throws WorkerException if resume fails
     */
    void resume() throws WorkerException;

    /**
     * Close this worker
     * @throws IOException if close fails
     */
    void close() throws IOException;

    /**
     * The shutdown is called when this worker is about to be removed from the system.
     * @throws IOException if shutdown fails
     */
    void shutdown() throws IOException;

}
