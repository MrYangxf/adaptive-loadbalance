package com.aliware.tianchi;

import com.aliware.tianchi.util.ThreadPoolStats;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.common.threadpool.support.AbortPolicyWithReport;
import org.apache.dubbo.common.utils.ConcurrentHashSet;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

import static com.aliware.tianchi.common.util.ObjectUtil.isNull;
import static com.aliware.tianchi.util.ThreadPoolUtil.getMaybeBlocker;

/**
 * todo:
 *
 * @author yangxf
 */
// @Adaptive
public class TestThreadPool implements ThreadPool {

    private final Set<Thread> threadSet = new ConcurrentHashSet<>();

    private ThreadPoolExecutor executor;

    private Object maybeBlocker;

    @Override
    public synchronized Executor getExecutor(URL url) {
        if (executor != null) {
            return executor;
        }

        String name = url.getParameter(Constants.THREAD_NAME_KEY, Constants.DEFAULT_THREAD_NAME);
        int threads = url.getParameter(Constants.THREADS_KEY, Constants.DEFAULT_THREADS);
        int queues = url.getParameter(Constants.QUEUES_KEY, Constants.DEFAULT_QUEUES);
        BlockingQueue<Runnable> workQueue = queues == 0 ? new SynchronousQueue<>() :
                (queues < 0 ? new LinkedBlockingQueue<>() : new LinkedBlockingQueue<>(queues));
        maybeBlocker = getMaybeBlocker(workQueue);
        executor = new StatsThreadPoolExecutor(threads, threads,
                                               0, TimeUnit.MILLISECONDS,
                                               workQueue,
                                               new StatsNamedThreadFactory(name, true),
                                               new AbortPolicyWithReport(name, url));
        return executor;
    }

    public ThreadPoolStats getThreadPoolStats() {
        int frees = 0, waits = 0, works = 0;
        for (Thread t : threadSet) {
            if (!t.isAlive()) {
                threadSet.remove(t);
                continue;
            }

            Thread.State state = t.getState();
            Object blocker;
            if (state != Thread.State.WAITING &&
                state != Thread.State.TIMED_WAITING ||
                isNull(blocker = LockSupport.getBlocker(t))) {
                works++;
                continue;
            }

            if (blocker == maybeBlocker) {
                frees++;
            } else {
                waits++;
            }
        }

        int finalFrees = frees;
        int finalWaits = waits;
        int finalWorks = works;
        return new ThreadPoolStats() {
            @Override
            public int freeCount() {
                return finalFrees;
            }

            @Override
            public int waitCount() {
                return finalWaits;
            }

            @Override
            public int workCount() {
                return finalWorks;
            }
        };
    }

    class StatsNamedThreadFactory extends NamedInternalThreadFactory {
        StatsNamedThreadFactory(String prefix, boolean daemon) {
            super(prefix, daemon);
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = super.newThread(runnable);
            threadSet.add(thread);
            return thread;
        }
    }

}
