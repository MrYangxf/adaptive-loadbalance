package com.aliware.tianchi.util;

/**
 * @author yangxf
 */
public interface ThreadPoolStats {

    int freeCount();

    int waitCount();

    int workCount();

    ThreadPoolStats EMPTY_THREAD_POOL_STATS = new ThreadPoolStats() {
        @Override
        public int freeCount() {
            return 0;
        }

        @Override
        public int waitCount() {
            return 0;
        }

        @Override
        public int workCount() {
            return 0;
        }
    };

}
