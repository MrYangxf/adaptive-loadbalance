package com.aliware.tianchi;

import java.io.IOException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author guohaoice@gmail.com
 */
public class MyConsumer {
  public static void main(String[] args) throws IOException {
    ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"classpath:dubbo-consumer.xml"});
    context.start();
    HashInterface bean = context.getBean(HashInterface.class);
    for (int i = 0; i < 10; i++) {
      System.out.println(bean.hash("hahaha"));
    }
    System.in.read(); // press any key to exit
  }
}
