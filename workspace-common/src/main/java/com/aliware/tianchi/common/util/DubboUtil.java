package com.aliware.tianchi.common.util;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.Arrays;

/**
 * @author yangxf
 */
public final class DubboUtil {

    private DubboUtil() {
        throw new InstantiationError("DubboUtil can't be instantiated");
    }

    public static String getIpAddress(Invoker<?> invoker) {
        URL url = invoker.getUrl();
        return url.getIp() + ':' + url.getPort();
    }

    public static String getServiceId(Invoker<?> invoker, Invocation invocation) {
        return invoker.getInterface().getName() + '#' +
               invocation.getMethodName() +
               Arrays.toString(invocation.getParameterTypes());
    }

}
