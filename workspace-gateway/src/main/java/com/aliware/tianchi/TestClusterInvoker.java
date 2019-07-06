package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author yangxf
 */
public class TestClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(org.apache.dubbo.rpc.cluster.support.FailoverClusterInvoker.class);

    public TestClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        List<Invoker<T>> copyInvokers = invokers;
        checkInvokers(copyInvokers, invocation);
        String methodName = RpcUtils.getMethodName(invocation);
        int len = getUrl().getMethodParameter(methodName, Constants.RETRIES_KEY, Constants.DEFAULT_RETRIES) + 1;
        if (len <= 0) {
            len = 1;
        }
        // retry loop.
        RpcException le = null; // last exception.
        List<Invoker<T>> invoked = new ArrayList<Invoker<T>>(copyInvokers.size()); // invoked invokers.
        Set<String> providers = new HashSet<String>(len);
        for (int i = 0; i < len; i++) {
            //Reselect before retry to avoid a change of candidate `invokers`.
            //NOTE: if `invokers` changed, then `invoked` also lose accuracy.
            if (i > 0) {
                checkWhetherDestroyed();
                copyInvokers = list(invocation);
                // check again
                checkInvokers(copyInvokers, invocation);
            }
            
            Invoker<T> invoker;
            try {
                invoker = select(loadbalance, invocation, copyInvokers, invoked);
            } catch (RpcException e) {
                RpcResult rpcResult = new RpcResult();
                rpcResult.setException(e);
                return rpcResult;
            }

            invoked.add(invoker);
            RpcContext.getContext().setInvokers((List) invoked);
            try {
                Result result = invoker.invoke(invocation);
                if (le != null && logger.isWarnEnabled()) {
                    logger.warn("Although retry the method " + methodName
                                + " in the service " + getInterface().getName()
                                + " was successful by the provider " + invoker.getUrl().getAddress()
                                + ", but there have been failed providers " + providers
                                + " (" + providers.size() + "/" + copyInvokers.size()
                                + ") from the registry " + directory.getUrl().getAddress()
                                + " on the consumer " + NetUtils.getLocalHost()
                                + " using the dubbo version " + Version.getVersion() + ". Last error is: "
                                + le.getMessage(), le);
                }
                return result;
            } catch (RpcException e) {
                if (e.isBiz()) { // biz exception.
                    throw e;
                }
                le = e;
            } catch (Throwable e) {
                le = new RpcException(e.getMessage(), e);
            } finally {
                providers.add(invoker.getUrl().getAddress());
            }
        }
        throw new RpcException(le.getCode(), "Failed to invoke the method "
                                             + methodName + " in the service " + getInterface().getName()
                                             + ". Tried " + len + " times of the providers " + providers
                                             + " (" + providers.size() + "/" + copyInvokers.size()
                                             + ") from the registry " + directory.getUrl().getAddress()
                                             + " on the consumer " + NetUtils.getLocalHost() + " using the dubbo version "
                                             + Version.getVersion() + ". Last error is: "
                                             + le.getMessage(), le.getCause() != null ? le.getCause() : le);
    }

}
