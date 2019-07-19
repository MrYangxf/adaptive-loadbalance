package com.aliware.tianchi.util;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

import static com.aliware.tianchi.common.util.ObjectUtil.checkNotNull;
import static com.aliware.tianchi.common.util.ObjectUtil.isNull;
import static com.aliware.tianchi.util.ThreadPoolStats.EMPTY_THREAD_POOL_STATS;

/**
 * @author yangxf
 */
public class ThreadPoolUtil {

    private ThreadPoolUtil() {
    }

    public static ThreadPoolStats getThreadPoolStats(Executor executor) {
        if (isNull(executor)) {
            return EMPTY_THREAD_POOL_STATS;
        }

        Set<Thread> threadSet = getWorkerThreads(executor);
        Object maybeBlocker = getMaybeBlocker(getWorkQueue(executor));

        int freeCount = 0, waitCount = 0, workCount = 0;

        for (Thread t : threadSet) {
            Thread.State state = t.getState();
            Object blocker;
            if (state != Thread.State.WAITING &&
                state != Thread.State.TIMED_WAITING ||
                isNull(blocker = LockSupport.getBlocker(t))) {
                workCount++;
                continue;
            }

            if (blocker == maybeBlocker) {
                freeCount++;
            } else {
                waitCount++;
            }
        }

        int finalFreeCount = freeCount;
        int finalWaitCount = waitCount;
        int finalWorkCount = workCount;
        return new ThreadPoolStats() {
            @Override
            public int freeCount() {
                return finalFreeCount;
            }

            @Override
            public int waitCount() {
                return finalWaitCount;
            }

            @Override
            public int workCount() {
                return finalWorkCount;
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static Set<Thread> getWorkerThreads(Executor executor) {
        checkNotNull(executor, "executor");
        Set<Thread> threadSet = new HashSet<>();
        try {
            if (ThreadPoolExecutor.class.isAssignableFrom(executor.getClass())) {
                HashSet<Object> workers = (HashSet<Object>) workersField.get(executor);
                for (Object worker : workers) {
                    threadSet.add((Thread) threadField.get(worker));
                }
            }
        } catch (Exception e) {
            // 
            e.printStackTrace();
        }
        return Collections.unmodifiableSet(threadSet);
    }

    @SuppressWarnings("unchecked")
    public static BlockingQueue<Runnable> getWorkQueue(Executor executor) {
        checkNotNull(executor, "executor");
        BlockingQueue<Runnable> queue = null;
        try {
            if (ThreadPoolExecutor.class.isAssignableFrom(executor.getClass())) {
                queue = (BlockingQueue<Runnable>) workQueueField.get(executor);
            }
        } catch (Exception e) {
            //
            e.printStackTrace();
        }
        return queue;
    }

    public static Object getMaybeBlocker(BlockingQueue<?> queue) {
        if (isNull(queue)) {
            return null;
        }

        Class<? extends BlockingQueue> queueClass = queue.getClass();

        if (queueClass == LinkedTransferQueue.class) {
            return queue;
        }

        Object blocker;
        try {
            if (queueClass == SynchronousQueue.class) {
                blocker = transfererField.get(queue);
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

    private static final Field workersField;
    private static final Field threadField;
    private static final Field workQueueField;
    private static final Field transfererField;

    static {
        try {
            workersField = ThreadPoolExecutor.class.getDeclaredField("workers");
            workersField.setAccessible(true);

            Class<?> workerClass = Class.forName("java.util.concurrent.ThreadPoolExecutor$Worker");
            threadField = workerClass.getDeclaredField("thread");
            threadField.setAccessible(true);

            workQueueField = ThreadPoolExecutor.class.getDeclaredField("workQueue");
            workQueueField.setAccessible(true);

            transfererField = SynchronousQueue.class.getDeclaredField("transferer");
            transfererField.setAccessible(true);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

}
