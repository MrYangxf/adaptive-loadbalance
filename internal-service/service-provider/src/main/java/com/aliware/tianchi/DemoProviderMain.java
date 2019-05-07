package com.aliware.tianchi;

import java.io.IOException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/** @author guohaoice@gmail.com */
public class DemoProviderMain {
  public static void main(String[] args) throws IOException {
    //      if(args.length!=3){
    //          throw new IllegalArgumentException("Bad argument number");
    //      }

    //      String salt=args[0];
    //      System.out.println(salt);
    new EmbeddedZooKeeper(2181, false).start();
    ClassPathXmlApplicationContext context =
        new ClassPathXmlApplicationContext(new String[] {"classpath:dubbo-provider.xml"});
    context.start();
    System.in.read(); // press any key to exit
  }
}
