package com.aliware.tianchi;

import java.io.IOException;
import java.util.List;
import com.aliware.tianchi.policy.SmallConfig;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author guohaoice@gmail.com */
public class DemoProviderMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(DemoProviderMain.class);

  public static void main(String[] args) throws IOException {
    String env;
    String salt;
    if (args.length != 3) {
      LOGGER.info("No specific args found, use [DEFAULT] to run demo provider");
      env = "small";
      salt = "salt_val";
    } else {
      env = args[0];
      salt = args[1];
    }
    List<ThrashConfig> configs;
    switch (env) {
      case "small":
        configs = new SmallConfig().allConfig;
        break;
      default:
        configs = new SmallConfig().allConfig;
    }

    // 当前应用配置
    ApplicationConfig application = new ApplicationConfig();
    application.setName("service-provider");

    // 连接注册中心配置
    RegistryConfig registry = new RegistryConfig();
    registry.setAddress("N/A");

    // 服务提供者协议配置
    ProtocolConfig protocol = new ProtocolConfig();
    protocol.setName("dubbo");
    protocol.setPort(20880);
    protocol.setThreads(200);

    // 注意：ServiceConfig为重对象，内部封装了与注册中心的连接，以及开启服务端口

    // 服务提供者暴露服务配置
    ServiceConfig<HashInterface> service =
        new ServiceConfig<>(); // 此实例很重，封装了与注册中心的连接，请自行缓存，否则可能造成内存和连接泄漏
    service.setApplication(application);
    service.setRegistry(registry); // 多个注册中心可以用setRegistries()
    service.setProtocol(protocol); // 多个协议可以用setProtocols()
    service.setInterface(HashInterface.class);
    service.setRef(new HashServiceImpl(salt, configs));

    // 暴露及注册服务
    service.export();
    System.in.read(); // press any key to exit
  }
}
