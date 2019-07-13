package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.common.threadpool.support.AbortPolicyWithReport;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

import static com.aliware.tianchi.common.util.ObjectUtil.isNull;
import static com.aliware.tianchi.common.util.ObjectUtil.nonNull;

/**
 * todo:
 *
 * @author yangxf
 */
@Adaptive
public class TestThreadPool implements ThreadPool {

    private final Map<String, Thread> threadMap = new ConcurrentHashMap<>();

    private ThreadPoolExecutor executor;

    @Override
    public synchronized Executor getExecutor(URL url) {
        if (executor != null) {
            return executor;
        }

        String name = url.getParameter(Constants.THREAD_NAME_KEY, Constants.DEFAULT_THREAD_NAME);
        int threads = url.getParameter(Constants.THREADS_KEY, Constants.DEFAULT_THREADS);
        int queues = url.getParameter(Constants.QUEUES_KEY, Constants.DEFAULT_QUEUES);
        executor =
                new StatsThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS,
                                            queues == 0 ? new SynchronousQueue<>() :
                                                    (queues < 0 ? new LinkedBlockingQueue<>()
                                                            : new LinkedBlockingQueue<>(queues)),
                                            new StatsNamedThreadFactory(name, true), new AbortPolicyWithReport(name, url));
        return executor;
    }

    public ThreadStats getThreadStats() {
        int queues = 0, waits = 0, works = 0;
        for (Map.Entry<String, Thread> entry : threadMap.entrySet()) {
            Thread t = entry.getValue();
            if (!t.isAlive()) {
                threadMap.remove(entry.getKey());
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
            
            Class<?> bClass = blocker.getClass();
            Class<?> declaringClass = bClass.getDeclaringClass();
            if (declaringClass == SynchronousQueue.class) {
                queues++;
            } else {
                waits++;
            }
        }

        int finalQueues = queues;
        int finalWaits = waits;
        int finalWorks = works;
        return new ThreadStats() {
            @Override
            public int queues() {
                return finalQueues;
            }

            @Override
            public int waits() {
                return finalWaits;
            }

            @Override
            public int works() {
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
            threadMap.put(thread.getName(), thread);
            return thread;
        }
    }

    interface ThreadStats {

        int queues();

        int waits();

        int works();
    }

}
