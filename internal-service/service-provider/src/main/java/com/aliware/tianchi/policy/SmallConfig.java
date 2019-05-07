package com.aliware.tianchi.policy;

import java.util.Arrays;
import java.util.List;
import com.aliware.tianchi.ThrashConfig;

/**
 * @author guohaoice@gmail.com
 */
public class SmallConfig {
    private final int baseRTT=20;
    private final int onePeriodInMs=20_000;
    private final int baseMaxConcurrency=10_000;
    private final ThrashConfig config1=new ThrashConfig(onePeriodInMs,baseMaxConcurrency,baseRTT/2);
    private final ThrashConfig config2=new ThrashConfig(onePeriodInMs,baseMaxConcurrency,baseRTT);
    private final ThrashConfig config3=new ThrashConfig(onePeriodInMs,baseMaxConcurrency/2,baseRTT);
    private final ThrashConfig config4=new ThrashConfig(onePeriodInMs,baseMaxConcurrency,baseRTT);
    private final ThrashConfig config5=new ThrashConfig(onePeriodInMs,baseMaxConcurrency/2,baseRTT*2);
    private final ThrashConfig config6=new ThrashConfig(onePeriodInMs,baseMaxConcurrency*2,baseRTT);
    private final ThrashConfig config7=new ThrashConfig(onePeriodInMs,baseMaxConcurrency,baseRTT);
    private final ThrashConfig config8=new ThrashConfig(onePeriodInMs,baseMaxConcurrency*2,baseRTT/2);
    private final ThrashConfig config9=new ThrashConfig(onePeriodInMs,baseMaxConcurrency,baseRTT);

    public final List<ThrashConfig> allConfig= Arrays.asList(config1,config2,config3,config4,config5,config6,config7,config8,config9);
}
