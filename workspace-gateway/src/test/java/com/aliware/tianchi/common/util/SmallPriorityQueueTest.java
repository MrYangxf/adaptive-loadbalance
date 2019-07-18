package com.aliware.tianchi.common.util;

import org.junit.Test;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class SmallPriorityQueueTest {

    @Test
    public void test() {
        int cap = 100;

        Queue<Long> q1 = new PriorityQueue<>();
        Queue<Long> q2 = new SmallPriorityQueue<>(cap);

        for (int i = 0; i < cap; i++) {
            long n = ThreadLocalRandom.current().nextLong();
            q1.offer(n);
            q2.offer(n);
        }

        while (!q2.isEmpty()) {
            assertEquals(q1.poll(), q2.poll());
        }
        
    }
}