package org.xbib.elasticsearch.common.task.node;

import org.xbib.elasticsearch.common.task.Task;

import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Task queue
 */
public class TaskRegistry {

    private final PriorityBlockingQueue<Task> queue = new PriorityBlockingQueue<Task>();

    public TaskRegistry() {
    }

    public void addTask(Task task) {
        queue.add(task);
    }

    public Queue<Task> getTasks() {
        return queue;
    }

}
