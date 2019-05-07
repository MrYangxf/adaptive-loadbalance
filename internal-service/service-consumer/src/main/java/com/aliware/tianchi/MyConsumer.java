package com.aliware.tianchi;

import java.io.IOException;
import java.util.Collections;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;


/** @author guohaoice@gmail.com */
public class MyConsumer {
  public static void main(String[] args) throws IOException, InterruptedException {
    // 当前应用配置
    ApplicationConfig application = new ApplicationConfig();
    application.setName("service-gateway");

    // 连接注册中心配置
    RegistryConfig registry = new RegistryConfig();
//    String address="com.aliware.tianchi.HashInterface-"
//            +"localhost:20880_"
//            +"localhost:20880";
//    registry.setAddress("list://localhost:9999");
    registry.setAddress("N/A");
//    registry.setParameters(Collections.singletonMap(LIST_URL_KEY,address));


    // 注意：ReferenceConfig为重对象，内部封装了与注册中心的连接，以及与服务提供方的连接

    // 引用远程服务
    ReferenceConfig<HashInterface> reference =
        new ReferenceConfig<HashInterface>(); // 此实例很重，封装了与注册中心的连接以及与提供者的连接，请自行缓存，否则可能造成内存和连接泄漏
    reference.setApplication(application);
    reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
    reference.setInterface(HashInterface.class);
//    reference.setUrl(new URL(Constants.DUBBO_PROTOCOL,"localhost",20880,reference.getInterface()));
    reference.setUrl("localhost:20880");

    // 和本地bean一样使用xxxService
    HashInterface service = reference.get(); // 注意：此代理对象内部封装了所有通讯细节，对象较重，请缓存复用
    while (true) {
      Thread.sleep(1000);
      System.out.println(service.hash("hahaha"));
    }
  }
}
