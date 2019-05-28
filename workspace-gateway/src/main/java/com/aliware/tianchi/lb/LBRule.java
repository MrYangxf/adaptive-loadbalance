package com.aliware.tianchi.lb;

import org.apache.dubbo.rpc.Invoker;

import java.util.List;

/**
 * @author yangxf
 */
public interface LBRule {

    <T> Invoker<T> select(List<Invoker<T>>  candidates);
    
}
