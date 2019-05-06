package com.aliware.tianchi;

import java.io.IOException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author guohaoice@gmail.com
 */
public class GatewayApp {
  public static void main(String[] args) throws IOException {
    ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"dubbo-provider.xml"});
    context.start();

    System.in.read(); // press any key to exit
  }
}
