/*
 * Copyright 2009 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package no.found.elasticsearch.transport.netty.ssl.internal;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public final class NonReentrantLock extends AbstractQueuedSynchronizer
        implements Lock {

    private Thread owner;

    public void lock() {
        acquire(1);
    }

    public void lockInterruptibly() throws InterruptedException {
        acquireInterruptibly(1);
    }

    public boolean tryLock() {
        return tryAcquire(1);
    }

    public boolean tryLock(long time, TimeUnit unit)
            throws InterruptedException {
        return tryAcquireNanos(1, unit.toNanos(time));
    }

    public void unlock() {
        release(1);
    }

    public boolean isHeldByCurrentThread() {
        return isHeldExclusively();
    }

    public Condition newCondition() {
        return new ConditionObject();
    }

    @Override
    protected boolean tryAcquire(int acquires) {
        if (compareAndSetState(0, 1)) {
            owner = Thread.currentThread();
            return true;
        }
        return false;
    }

    @Override
    protected boolean tryRelease(int releases) {
        if (Thread.currentThread() != owner) {
            throw new IllegalMonitorStateException();
        }
        owner = null;
        setState(0);
        return true;
    }

    @Override
    protected boolean isHeldExclusively() {
        return getState() != 0 && owner == Thread.currentThread();
    }
}
