package com.aliware.tianchi;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Facade
 *
 * @author guohaoice@gmail.com
 */
public class HashServiceImpl implements HashInterface {
  private static final Logger LOGGER = LoggerFactory.getLogger(HashServiceImpl.class);

  private final String salt;
  private final AtomicBoolean init = new AtomicBoolean(false);
  private final List<ThrashConfig> configs;
  private volatile ThrashConfig config;
  private Random rng = new Random(2019);
  private ScheduledExecutorService scheduler =
      new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("HashService-Refresher"));

  public HashServiceImpl(String salt) {
    this(salt, Collections.emptyList());
  }

  public HashServiceImpl(String salt, List<ThrashConfig> configs) {
    this.salt = salt;
    this.config = ThrashConfig.INIT_CONFIG;
    this.configs = Collections.unmodifiableList(configs);
  }

  @Override
  public int hash(String input) {
    long st = System.currentTimeMillis();
    if (!init.get()) {
      if (init.compareAndSet(false, true)) {
        int startTime = 0;
        for (ThrashConfig thrashConfig : configs) {
          scheduler.schedule(
              () -> refresh(thrashConfig), startTime + config.durationInMs, TimeUnit.MILLISECONDS);
          startTime += config.durationInMs;
        }
      }
    }
    Semaphore permit = config.permit;
    try {
      permit.acquire();
      long baseRtt = nextRTT();
      Thread.sleep(baseRtt);
      return (input + salt).hashCode();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      long cost = System.currentTimeMillis() - st;
      LOGGER.info("HashService cost:{} ms to handle request", cost);
      permit.release();
    }
    throw new IllegalStateException("Unexpected exception");
  }

  private void refresh(ThrashConfig thrashConfig) {
    this.config = thrashConfig;
  }

  private long nextRTT() {
    double u = rng.nextDouble();
    int x = 0;
    double cdf = 0;
    while (u >= cdf) {
      x++;
      cdf = 1 - Math.exp(-1.0D * 1 / config.averageRTTInMs * x);
    }
    return x;
  }
}
