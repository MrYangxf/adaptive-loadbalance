package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Dispatcher;

/**
 * @author yangxf
 */
// @Adaptive
public class TestDispatcher implements Dispatcher {
    @Override
    public ChannelHandler dispatch(ChannelHandler handler, URL url) {
        return new TestChannelHandler(handler, url);
    }
}
