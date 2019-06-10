package com.aliware.tianchi.lb.metric.instance;

import com.aliware.tianchi.lb.metric.ServerStats;
import com.aliware.tianchi.lb.metric.TimeWindowInstanceStats;

import java.util.concurrent.ThreadLocalRandom;

public class TimeWindowInstanceStatsTest {

    public static void main(String[] args) throws InterruptedException {
        int s = 5;
        TimeWindowInstanceStats stats = new TimeWindowInstanceStats(s, new ServerStats(""), "");
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                for (int j = 0; j < 1000000; j++) {
                    int r = ThreadLocalRandom.current().nextInt(100);

                    if (r < 80) {
                        stats.success(ThreadLocalRandom.current().nextInt(500));
                    } else if (r < 95) {
                        stats.failure(ThreadLocalRandom.current().nextInt(800));
                    } else {
                        stats.rejection();
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        for (; ; ) {
            System.out.println("最近" + s + "秒");
            System.out.println("平均响应时间 ：" + stats.getAvgResponseMs());
            System.out.println("吞吐量      ：" + stats.getThroughput());
            System.out.println("请求数      ：" + stats.getNumberOfRequests());
            System.out.println("错误数      ：" + stats.getNumberOfFailures());
            System.out.println("拒绝数      ：" + stats.getNumberOfRejections());
            stats.clean();
            System.out.println("----------");
            Thread.sleep(2000);
        }

    }
}