// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.curvine.executor;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicInteger;

abstract public class AbstractExecutorService extends java.util.concurrent.AbstractExecutorService {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final String namePrefix;
    protected final int threadNum;

    protected final int queueSize;

    protected final int waitTimeout;

    protected List<Thread> threads;

    protected final AtomicInteger ctl = new AtomicInteger(RUNNING);

    public final static int RUNNING = -1;
    public final static int SHUTDOWN = 0;
    public final static int STOP = 1;

    public AbstractExecutorService(String namePrefix, int threadNum, int queueSize, int waitTimeout) {
        if (StringUtils.isEmpty(namePrefix)) {
            this.namePrefix = this.getClass().getSimpleName();
        } else {
            this.namePrefix = namePrefix;
        }
        this.threadNum = threadNum;
        this.queueSize = queueSize;
        this.waitTimeout = waitTimeout;
        this.threads = new ArrayList<>(threadNum);

        logger.info("{} create success, threadNum: {}, queueSize {}",
                getClass().getSimpleName(), threadNum, queueSize);
    }

    public AbstractExecutorService(int threadNum, int queueSize, int waitTimeout) {
        this(null, threadNum, queueSize, waitTimeout);
    }

    public abstract void execute(int value, Runnable command);

    public int size(int value) {
        throw new ExecutorException("Execute thread index is required!");
    }

    @Override
    public void execute(Runnable runnable) {
        throw new ExecutorException("Execute thread index is required!");
    }

    public Future<?> submit(int value, Runnable command) {
        if (command == null) {
            throw new NullPointerException();
        }
        RunnableFuture<Void> ftask = newTaskFor(command, null);
        execute(value, ftask);
        return ftask;
    }

    public <T> Future<T> submit(int value, Callable<T> task) {
        if (task == null) {
            throw new NullPointerException();
        }
        RunnableFuture<T> ftask = newTaskFor(task);
        execute(value, ftask);
        return ftask;
    }

    public int getIndex(int value) {
        return Math.abs(value % threadNum);
    }

    public Thread getThread(int value) {
        return threads.get(getIndex(value));
    }

    public String threadName(int i) {
        return namePrefix + "-" + i;
    }

    public int getActiveCount() {
        return threadNum;
    }

    public int getPoolSize() {
        return threadNum;
    }

    public int getMaximumPoolSize() {
        return threadNum;
    }

    public int allowsCoreThreadTimeOut() {
        return 0;
    }

    public boolean isRunning() {
        return ctl.get() == RUNNING;
    }

    @Override
    public boolean isShutdown() {
        return !isRunning();
    }

    public boolean isStop() {
        return ctl.get() == STOP;
    }

    private static boolean runStateAtLeast(int c, int s) {
        return c >= s;
    }

    protected void advanceRunState(int targetState) {
        for (;;) {
            int c = ctl.get();
            if (runStateAtLeast(c, targetState) || ctl.compareAndSet(c, targetState)) {
                break;
            }
        }
    }
}
