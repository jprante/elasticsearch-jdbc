package org.xbib.elasticsearch.common.task.worker.job;

public interface JobExecutionListener {

    void beforeBegin(Job job);

    void onSuccess(Job job);

    void onFailure(Job job);
}
