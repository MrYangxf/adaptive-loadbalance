package com.aliware.tianchi;

import java.util.List;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;

/**
 * Gateway 启动入口
 * @author guohaoice@gmail.com */
public class MyConsumer {
  public static void main(String[] args) throws InterruptedException {
    ApplicationConfig application = new ApplicationConfig();
    application.setName("service-gateway");

    // 直连方式，不使用注册中心
    RegistryConfig registry = new RegistryConfig();
    registry.setAddress("N/A");

    ReferenceConfig<HashInterface> reference = new ReferenceConfig<>();
    reference.setApplication(application);
    reference.setRegistry(registry);
    reference.setInterface(HashInterface.class);

    List<URL> urls = reference.toUrls();
    // 添加直连的 provider 地址
    urls.add(new URL(Constants.DUBBO_PROTOCOL, "localhost", 20880, reference.getInterface()));
    urls.add(new URL(Constants.DUBBO_PROTOCOL, "localhost", 20880, reference.getInterface()));

    HashInterface service = reference.get();
    while (true) {
      Thread.sleep(1000);
      System.out.println(service.hash("hahaha"));
    }
  }
}
