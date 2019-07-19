package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.common.threadpool.support.AbortPolicyWithReport;
import org.apache.dubbo.common.utils.ConcurrentHashSet;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

import static com.aliware.tianchi.common.util.ObjectUtil.isNull;

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

    public ThreadStats getThreadStats() {
        int queues = 0, waits = 0, works = 0;
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

    private static Object getMaybeBlocker(BlockingQueue<?> queue) {
        Class<? extends BlockingQueue> queueClass = queue.getClass();

        if (queueClass == LinkedTransferQueue.class) {
            return queue;
        }

        Object blocker;
        try {
            if (queueClass == SynchronousQueue.class) {
                Field field = SynchronousQueue.class.getDeclaredField("transferer");
                field.setAccessible(true);
                blocker = field.get(queue);
            } else {
                Field field = queueClass.getDeclaredField("notEmpty");
                field.setAccessible(true);
                blocker = field.get(queue);
            }
        } catch (Exception e) {
            blocker = null;
        }

        return blocker;
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

    interface ThreadStats {

        int queues();

        int waits();

        int works();
    }

}
