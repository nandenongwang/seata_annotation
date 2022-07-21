/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.core.rpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.seata.common.exception.FrameworkErrorCode;
import io.seata.common.exception.FrameworkException;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.thread.PositiveAtomicCounter;
import io.seata.core.protocol.*;
import io.seata.core.protocol.transaction.AbstractTransactionResponse;
import io.seata.core.rpc.Disposable;
import io.seata.core.rpc.hook.RpcHook;
import io.seata.core.rpc.processor.Pair;
import io.seata.core.rpc.processor.RemotingProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.SocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 远程调用基类、提供一些基础公共,如:
 * 发送命令
 * 接受命令委托处理器处理
 * 请求future超时检测
 * rpc钩子
 * The abstract netty remoting.
 *
 * @author slievrly
 * @author zhangchenghui.dev@gmail.com
 */
public abstract class AbstractNettyRemoting implements Disposable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNettyRemoting.class);

    /**
     * 请求超时检测线程池
     * The Timer executor.
     */
    protected final ScheduledExecutorService timerExecutor = new ScheduledThreadPoolExecutor(1,
            new NamedThreadFactory("timeoutChecker", 1, true));

    /**
     * 工作线程池
     * The Message executor.
     */
    protected final ThreadPoolExecutor messageExecutor;

    /**
     * 请求ID生成器
     * Id generator of this remoting
     */
    protected final PositiveAtomicCounter idGenerator = new PositiveAtomicCounter();

    /**
     * 挂起的future请求
     * Obtain the return result through MessageFuture blocking.
     *
     * @see AbstractNettyRemoting#sendSync
     */
    protected final ConcurrentHashMap<Integer, MessageFuture> futures = new ConcurrentHashMap<>();

    private static final long NOT_WRITEABLE_CHECK_MILLS = 10L;

    /**
     * The Now mills.
     */
    protected volatile long nowMills = 0;
    private static final int TIMEOUT_CHECK_INTERVAL = 3000;
    protected final Object lock = new Object();

    /**
     * 批量消息是否发送中标识
     * The Is sending.
     */
    protected volatile boolean isSending = false;
    private String group = "DEFAULT";

    /**
     * 处理指令及其处理器
     * This container holds all processors.
     * processor type {@link MessageType}
     */
    protected final HashMap<Integer/*MessageType*/, Pair<RemotingProcessor, ExecutorService>> processorTable = new HashMap<>(32);

    /**
     * RPC钩子
     */
    protected final List<RpcHook> rpcHooks = EnhancedServiceLoader.loadAll(RpcHook.class);

    /**
     * 初始化请求future超时检测定时任务
     */
    public void init() {
        timerExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for (Map.Entry<Integer, MessageFuture> entry : futures.entrySet()) {
                    MessageFuture future = entry.getValue();
                    if (future.isTimeout()) {
                        futures.remove(entry.getKey());
                        RpcMessage rpcMessage = future.getRequestMessage();
                        future.setResultMessage(new TimeoutException(String
                                .format("msgId: %s ,msgType: %s ,msg: %s ,request timeout", rpcMessage.getId(), String.valueOf(rpcMessage.getMessageType()), rpcMessage.getBody().toString())));
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("timeout clear future: {}", entry.getValue().getRequestMessage().getBody());
                        }
                    }
                }

                nowMills = System.currentTimeMillis();
            }
        }, TIMEOUT_CHECK_INTERVAL, TIMEOUT_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public AbstractNettyRemoting(ThreadPoolExecutor messageExecutor) {
        this.messageExecutor = messageExecutor;
    }

    public int getNextMessageId() {
        return idGenerator.incrementAndGet();
    }

    public ConcurrentHashMap<Integer, MessageFuture> getFutures() {
        return futures;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public void destroyChannel(Channel channel) {
        destroyChannel(getAddressFromChannel(channel), channel);
    }

    @Override
    public void destroy() {
        timerExecutor.shutdown();
        messageExecutor.shutdown();
    }

    /**
     * 同步发送消息
     * rpc sync request
     * Obtain the return result through MessageFuture blocking.
     *
     * @param channel       netty channel
     * @param rpcMessage    rpc message
     * @param timeoutMillis rpc communication timeout
     * @return response message
     * @throws TimeoutException
     */
    protected Object sendSync(Channel channel, RpcMessage rpcMessage, long timeoutMillis) throws TimeoutException {
        if (timeoutMillis <= 0) {
            throw new FrameworkException("timeout should more than 0ms");
        }
        if (channel == null) {
            LOGGER.warn("sendSync nothing, caused by null channel.");
            return null;
        }

        MessageFuture messageFuture = new MessageFuture();
        messageFuture.setRequestMessage(rpcMessage);
        messageFuture.setTimeout(timeoutMillis);
        futures.put(rpcMessage.getId(), messageFuture);

        channelWritableCheck(channel, rpcMessage.getBody());

        String remoteAddr = ChannelUtil.getAddressFromChannel(channel);
        doBeforeRpcHooks(remoteAddr, rpcMessage);

        //打印发送请求
        if (rpcMessage.getBody() instanceof MessageTypeAware) {
            MessageTypeAware messageTypeAware = (MessageTypeAware) rpcMessage.getBody();
            System.out.println("【同步】发送指令至[" + remoteAddr + "]: " + messageTypeAware.getTypeCode() + ", 指令名: 【" + MessageType.toName(messageTypeAware.getTypeCode()) + "】" + new Date());
        }

        channel.writeAndFlush(rpcMessage).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                MessageFuture messageFuture1 = futures.remove(rpcMessage.getId());
                if (messageFuture1 != null) {
                    messageFuture1.setResultMessage(future.cause());
                }
                destroyChannel(future.channel());
            }
        });

        try {
            Object result = messageFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
            doAfterRpcHooks(remoteAddr, rpcMessage, result);

            //打印请求响应
            if (rpcMessage.getBody() instanceof MessageTypeAware) {
                AbstractTransactionResponse response = (AbstractTransactionResponse) result;
                MessageTypeAware messageTypeAware = (MessageTypeAware) rpcMessage.getBody();
                System.out.println("【同步响应】发送指令至[" + remoteAddr + "]: " + messageTypeAware.getTypeCode() +
                        ", 指令名: 【" + MessageType.toName(messageTypeAware.getTypeCode()) + "】,响应结果code:[" + response.getResultCode().name() + "]" + new Date());
            }


            return result;
        } catch (Exception exx) {
            LOGGER.error("wait response error:{},ip:{},request:{}", exx.getMessage(), channel.remoteAddress(),
                    rpcMessage.getBody());
            if (exx instanceof TimeoutException) {
                throw (TimeoutException) exx;
            } else {
                throw new RuntimeException(exx);
            }
        }
    }

    /**
     * 异步发送消息
     * rpc async request.
     *
     * @param channel    netty channel
     * @param rpcMessage rpc message
     */
    protected void sendAsync(Channel channel, RpcMessage rpcMessage) {
        channelWritableCheck(channel, rpcMessage.getBody());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("write message:" + rpcMessage.getBody() + ", channel:" + channel + ",active?"
                    + channel.isActive() + ",writable?" + channel.isWritable() + ",isopen?" + channel.isOpen());
        }

        doBeforeRpcHooks(ChannelUtil.getAddressFromChannel(channel), rpcMessage);

        if (rpcMessage.getBody() instanceof MessageTypeAware && !(rpcMessage.getBody() instanceof MergeMessage)) {
            MessageTypeAware messageTypeAware = (MessageTypeAware) rpcMessage.getBody();
            System.out.println("【异步】发送指令至[" + channel.remoteAddress() + "]: " + messageTypeAware.getTypeCode() + ", 指令名: 【" + MessageType.toName(messageTypeAware.getTypeCode()) + "】");
        }

        if (rpcMessage.getBody() instanceof MergeMessage && !(rpcMessage.getBody() instanceof MergeResultMessage)) {
            MessageTypeAware messageTypeAware = (MessageTypeAware) rpcMessage.getBody();
            MergedWarpMessage mergeMessage = (MergedWarpMessage) rpcMessage.getBody();
            System.out.println("【异步】发送批量指令至[" + channel.remoteAddress() + "]: " + messageTypeAware.getTypeCode() + ", 指令名: 【" + MessageType.toName(messageTypeAware.getTypeCode()) + "】");
            System.out.println("--｛");
            mergeMessage.msgs.forEach(i -> {
                System.out.println("发送子指令至[" + channel.remoteAddress() + "]: " + i.getTypeCode() + ", 子指令名: 【" + MessageType.toName(i.getTypeCode()) + "】");
            });
            System.out.println("｝--");
        }

        channel.writeAndFlush(rpcMessage).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                destroyChannel(future.channel());
            }
        });
    }

    /**
     * 创建请求消息RPC包装
     */
    protected RpcMessage buildRequestMessage(Object msg, byte messageType) {
        RpcMessage rpcMessage = new RpcMessage();
        rpcMessage.setId(getNextMessageId());
        rpcMessage.setMessageType(messageType);
        rpcMessage.setCodec(ProtocolConstants.CONFIGURED_CODEC);
        rpcMessage.setCompressor(ProtocolConstants.CONFIGURED_COMPRESSOR);
        rpcMessage.setBody(msg);
        return rpcMessage;
    }

    /**
     * 创建响应消息RPC包装
     */
    protected RpcMessage buildResponseMessage(RpcMessage rpcMessage, Object msg, byte messageType) {
        RpcMessage rpcMsg = new RpcMessage();
        rpcMsg.setMessageType(messageType);
        rpcMsg.setCodec(rpcMessage.getCodec()); // same with request
        rpcMsg.setCompressor(rpcMessage.getCompressor());
        rpcMsg.setBody(msg);
        rpcMsg.setId(rpcMessage.getId());
        return rpcMsg;
    }

    /**
     * 接收消息处理、线程池拒绝后是否dump堆栈信息
     * For testing. When the thread pool is full, you can change this variable and share the stack
     */
    boolean allowDumpStack = false;

    /**
     * 处理接收到的消息【根据请求类型匹配处理器、委托处理】
     * Rpc message processing.
     *
     * @param ctx        Channel handler context.
     * @param rpcMessage rpc message.
     * @throws Exception throws exception process message error.
     * @since 1.3.0
     */
    protected void processMessage(ChannelHandlerContext ctx, RpcMessage rpcMessage) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("%s msgId:%s, body:%s", this, rpcMessage.getId(), rpcMessage.getBody()));
        }
        Object body = rpcMessage.getBody();
        if (body instanceof MessageTypeAware) {
            MessageTypeAware messageTypeAware = (MessageTypeAware) body;
            SocketAddress clientAddr = ctx.channel().remoteAddress();
            System.out.println("收到指令从[" + clientAddr + "] " + messageTypeAware.getTypeCode() + ", 指令名: " + MessageType.toName(messageTypeAware.getTypeCode()));
            final Pair<RemotingProcessor, ExecutorService> pair = this.processorTable.get((int) messageTypeAware.getTypeCode());
            if (pair != null) {
                if (pair.getSecond() != null) {
                    try {
                        pair.getSecond().execute(() -> {
                            try {
                                pair.getFirst().process(ctx, rpcMessage);
                            } catch (Throwable th) {
                                LOGGER.error(FrameworkErrorCode.NetDispatch.getErrCode(), th.getMessage(), th);
                            } finally {
                                MDC.clear();
                            }
                        });
                    } catch (RejectedExecutionException e) {
                        LOGGER.error(FrameworkErrorCode.ThreadPoolFull.getErrCode(),
                                "thread pool is full, current max pool size is " + messageExecutor.getActiveCount());
                        if (allowDumpStack) {
                            String name = ManagementFactory.getRuntimeMXBean().getName();
                            String pid = name.split("@")[0];
                            long idx = System.currentTimeMillis();
                            try {
                                String jstackFile = idx + ".log";
                                LOGGER.info("jstack command will dump to " + jstackFile);
                                Runtime.getRuntime().exec(String.format("jstack %s > %s", pid, jstackFile));
                            } catch (IOException exx) {
                                LOGGER.error(exx.getMessage());
                            }
                            allowDumpStack = false;
                        }
                    }
                } else {
                    try {
                        pair.getFirst().process(ctx, rpcMessage);
                    } catch (Throwable th) {
                        LOGGER.error(FrameworkErrorCode.NetDispatch.getErrCode(), th.getMessage(), th);
                    }
                }
            } else {
                LOGGER.error("This message type [{}] has no processor.", messageTypeAware.getTypeCode());
            }
        } else {
            LOGGER.error("This rpcMessage body[{}] is not MessageTypeAware type.", body);
        }
    }

    //region 获取channel地址、检查是否可写、销毁回调、RPC回调等方法

    /**
     * Gets address from context.
     *
     * @param ctx the ctx
     * @return the address from context
     */
    protected String getAddressFromContext(ChannelHandlerContext ctx) {
        return getAddressFromChannel(ctx.channel());
    }

    /**
     * Gets address from channel.
     *
     * @param channel the channel
     * @return the address from channel
     */
    protected String getAddressFromChannel(Channel channel) {
        SocketAddress socketAddress = channel.remoteAddress();
        String address = socketAddress.toString();
        if (socketAddress.toString().indexOf(NettyClientConfig.getSocketAddressStartChar()) == 0) {
            address = socketAddress.toString().substring(NettyClientConfig.getSocketAddressStartChar().length());
        }
        return address;
    }

    private void channelWritableCheck(Channel channel, Object msg) {
        int tryTimes = 0;
        synchronized (lock) {
            while (!channel.isWritable()) {
                try {
                    tryTimes++;
                    if (tryTimes > NettyClientConfig.getMaxNotWriteableRetry()) {
                        destroyChannel(channel);
                        throw new FrameworkException("msg:" + ((msg == null) ? "null" : msg.toString()),
                                FrameworkErrorCode.ChannelIsNotWritable);
                    }
                    lock.wait(NOT_WRITEABLE_CHECK_MILLS);
                } catch (InterruptedException exx) {
                    LOGGER.error(exx.getMessage());
                }
            }
        }
    }

    /**
     * Destroy channel.
     *
     * @param serverAddress the server address
     * @param channel       the channel
     */
    public abstract void destroyChannel(String serverAddress, Channel channel);

    protected void doBeforeRpcHooks(String remoteAddr, RpcMessage request) {
        for (RpcHook rpcHook : rpcHooks) {
            rpcHook.doBeforeRequest(remoteAddr, request);
        }
    }

    protected void doAfterRpcHooks(String remoteAddr, RpcMessage request, Object response) {
        for (RpcHook rpcHook : rpcHooks) {
            rpcHook.doAfterResponse(remoteAddr, request, response);
        }
    }
    //endregion
}
