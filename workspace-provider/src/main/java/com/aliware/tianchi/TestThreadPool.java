package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.common.threadpool.support.AbortPolicyWithReport;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author yangxf
 */
@Adaptive
public class TestThreadPool implements ThreadPool {

    @Override
    public Executor getExecutor(URL url) {
        System.out.println("PUSH HE HE");
        // String name = url.getParameter(Constants.THREAD_NAME_KEY, Constants.DEFAULT_THREAD_NAME);
        // int threads = url.getParameter(Constants.THREADS_KEY, Constants.DEFAULT_THREADS);
        // int queues = url.getParameter(Constants.QUEUES_KEY, Constants.DEFAULT_QUEUES);
        // int coreSize = Runtime.getRuntime().availableProcessors() * 2;
        // return new StatsThreadPoolExecutor(coreSize, coreSize, 0, TimeUnit.MILLISECONDS,
        //                               new ArrayBlockingQueue<>(threads + queues - coreSize),
        //                               new NamedInternalThreadFactory(name, true), new AbortPolicyWithReport(name, url));
        String name = url.getParameter(Constants.THREAD_NAME_KEY, Constants.DEFAULT_THREAD_NAME);
        int threads = url.getParameter(Constants.THREADS_KEY, Constants.DEFAULT_THREADS);
        int queues = url.getParameter(Constants.QUEUES_KEY, Constants.DEFAULT_QUEUES);
        return new StatsThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS,
                                           queues == 0 ? new SynchronousQueue<>() :
                                                   (queues < 0 ? new LinkedBlockingQueue<>()
                                                           : new LinkedBlockingQueue<>(queues)),
                                           new NamedInternalThreadFactory(name, true), new AbortPolicyWithReport(name, url));
    }

}
