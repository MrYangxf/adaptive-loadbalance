package com.aliware.tianchi;

import java.io.IOException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author guohaoice@gmail.com
 */
public class MyConsumer {
  public static void main(String[] args) throws IOException, InterruptedException {
    ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"classpath:dubbo-consumer.xml"});
    context.start();
    HashInterface bean = context.getBean(HashInterface.class);
    while(true){
      Thread.sleep(1000);
      System.out.println(bean.hash("hahaha"));
    }
//    System.in.read(); // press any key to exit
  }
}
