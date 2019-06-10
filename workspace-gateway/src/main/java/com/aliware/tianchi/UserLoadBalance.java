package com.aliware.tianchi;

import com.aliware.tianchi.lb.LBRule;
import com.aliware.tianchi.lb.rule.SelfAdaptiveRule;
import com.aliware.tianchi.lb.rule.WeightedLoadAndErrorRateRule;
import com.aliware.tianchi.lb.rule.WeightedLoadRule;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

import static com.aliware.tianchi.common.util.ObjectUtil.isEmpty;

/**
 * @author daofeng.xjf
 * <p>
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {

    private LBRule rule = new SelfAdaptiveRule();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        if (isEmpty(invokers)) {
            return null;
        }
        if (invokers.size() == 1) {
            return invokers.get(0);
        }

        return rule.select(invokers);
    }
}
