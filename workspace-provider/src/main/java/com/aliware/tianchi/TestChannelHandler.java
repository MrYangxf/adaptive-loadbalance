package com.aliware.tianchi;

import com.aliware.tianchi.common.util.Sequence;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.ExecutionException;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.transport.RequestLimiter;
import org.apache.dubbo.remoting.transport.dispatcher.ChannelEventRunnable;
import org.apache.dubbo.remoting.transport.dispatcher.WrappedChannelHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static org.apache.dubbo.remoting.transport.dispatcher.ChannelEventRunnable.ChannelState.RECEIVED;

/**
 * @author yangxf
 */
public class TestChannelHandler extends WrappedChannelHandler {

    private final List<RequestLimiter> requestLimiterSet = new ArrayList<>();

    private final Sequence adder = new Sequence(0);

    public TestChannelHandler(ChannelHandler handler, URL url) {
        super(handler, url);
        Set<String> supportedExtensions =
                ExtensionLoader.getExtensionLoader(RequestLimiter.class).getSupportedExtensions();
        for (String supportedExtension : supportedExtensions) {
            RequestLimiter requestLimiter =
                    ExtensionLoader.getExtensionLoader(RequestLimiter.class).getExtension(supportedExtension);
            requestLimiterSet.add(requestLimiter);
        }
    }

    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        if (message instanceof Request) {
            for (RequestLimiter requestLimiter : requestLimiterSet) {
                if (!requestLimiter.tryAcquire((Request) message, (int) adder.getValue())) {
                    Request request = (Request) message;
                    if (request.isTwoWay()) {
                        String msg = "TEST Server side(" + url.getIp() + "," + url.getPort() + ") request limiter acquired failed";
                        Response response = new Response(request.getId(), request.getVersion());
                        response.setStatus(Response.SERVER_REQUEST_LIMIT);
                        response.setErrorMessage(msg);
                        channel.send(response);
                        return;
                    }
                    throw new ExecutionException(message, channel, getClass() + " error when process received event .");
                }
            }
        }
        ExecutorService executor = getExecutorService();
        try {
            // ChannelEventRunnable h = new ChannelEventRunnable(channel, this.handler, RECEIVED, message);
            // adder.incrementAndGet();
            // CompletableFuture.runAsync(h, executor)
            //                  .thenRunAsync(adder::decrementAndGet, executor);
            executor.execute(() -> {
                adder.incrementAndGet();
                try {
                    handler.received(channel, message);
                } catch (Exception e) {
                    logger.warn("ChannelEventRunnable handle " + RECEIVED + " operation error, channel is " + channel
                                + ", message is " + message, e);
                } finally {
                    adder.decrementAndGet();
                }
            });
        } catch (Throwable t) {
            adder.decrementAndGet();
            //TODO A temporary solution to the problem that the exception information can not be sent to the opposite end after the thread pool is full. Need a refactoring
            //fix The thread pool is full, refuses to call, does not return, and causes the consumer to wait for time out
            if (message instanceof Request && t instanceof RejectedExecutionException) {
                Request request = (Request) message;
                if (request.isTwoWay()) {
                    String msg = "Server side(" + url.getIp() + "," + url.getPort() + ") threadpool is exhausted ,detail msg:" + t
                            .getMessage();
                    Response response = new Response(request.getId(), request.getVersion());
                    response.setStatus(Response.SERVER_THREADPOOL_EXHAUSTED_ERROR);
                    response.setErrorMessage(msg);
                    channel.send(response);
                    return;
                }
            }
            throw new ExecutionException(message, channel, getClass() + " error when process received event .", t);
        }
    }
}
