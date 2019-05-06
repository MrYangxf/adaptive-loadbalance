package com.aliware.tianchi;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Hash service impl
 *
 * @author guohaoice@gmail.com
 */
public class HashServiceImpl implements HashInterface {
  private long averageRTT;
  private int maxConcurrency;
  private String salt;

  public HashServiceImpl(long averageRTT, int maxConcurrency, String salt) {
    this.averageRTT = averageRTT;
    this.maxConcurrency = maxConcurrency;
    this.salt = salt;
  }

  @Override
  public int hash(String input) {
    long baseRtt = nextRTT();
    try {
      Thread.sleep(baseRtt);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return (input + salt).hashCode();
  }

  private long nextRTT() {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    double u = rng.nextDouble();
    int x = 0;
    double cdf = 0;
    while (u >= cdf) {
      x++;
      cdf = 1 - Math.exp(-1.0D * 1 / averageRTT * x);
    }
    return x;
  }
}
