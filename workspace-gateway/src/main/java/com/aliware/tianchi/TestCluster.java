package com.aliware.tianchi;

import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Directory;

/**
 * @author yangxf
 */
@Adaptive
public class TestCluster implements Cluster {
    @Override
    public <T> Invoker<T> join(Directory<T> directory) throws RpcException {
        return new TestClusterInvoker<>(directory);
    }
}
