/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.nio;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.ServerChannel;

import java.io.IOException;
import java.net.PortUnreachableException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link AbstractNioChannel} base class for {@link Channel}s that operate on messages.
 */
public abstract class AbstractNioMessageChannel extends AbstractNioChannel {
    boolean inputShutdown;

    /**
     * @see {@link AbstractNioChannel#AbstractNioChannel(Channel, SelectableChannel, int)}
     */
    protected AbstractNioMessageChannel(Channel parent, SelectableChannel ch, int readInterestOp) {
        super(parent, ch, readInterestOp);
    }

    @Override
    protected AbstractNioUnsafe newUnsafe() {
        return new NioMessageUnsafe();
    }

    @Override
    protected void doBeginRead() throws Exception {
        if (inputShutdown) {
            return;
        }
        super.doBeginRead();
    }

    /**
     * 服务端Unsafe负责读取连接
     */
    private final class NioMessageUnsafe extends AbstractNioUnsafe {

        private final List<Object> readBuf = new ArrayList<Object>();

        @Override
        public void read() {
            // 首先判断是否为当前EventLoop，即是否为当前线程
            assert eventLoop().inEventLoop();
            // config/pipeline为服务端Channel的属性
            final ChannelConfig config = config();
            final ChannelPipeline pipeline = pipeline();
            // 处理服务端接收速率
            final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
            allocHandle.reset(config);

            boolean closed = false;
            Throwable exception = null;
            try {
                try {
                    do {
                        /**
                         * 通过服务端底层JDK Channel accept()方法创建客户端JDK Channel
                         * SocketChannel ch = javaChannel().accept();
                         *
                         * readBuf -> 服务端Channel MessageUnsafe对应的一个field，用来临时保存读到的连接NioSocketChannel
                         * buf.add(new NioSocketChannel(this, ch));
                         */
                        int localRead = doReadMessages(readBuf);
                        if (localRead == 0) {
                            break;
                        }
                        if (localRead < 0) {
                            closed = true;
                            break;
                        }

                        allocHandle.incMessagesRead(localRead);
                    } while (allocHandle.continueReading());
                } catch (Throwable t) {
                    exception = t;
                }
                /**
                 * IMPORTANT: 新连接绑定NioEventLoop/注册Selector
                 * readBuf -> List<NioSocketChannel>
                 * Head -> ServerBootstrapAcceptor -> Tail
                 * fireChannelRead -> ServerBootstrapAcceptor
                 * - 添加childHandler至新连接的pipeline
                 * - 设置options和attrs
                 * - 选择NioEventLoop并注册Selector
                 *
                 * stack:
                 * register0:496, AbstractChannel$AbstractUnsafe (io.netty.channel)
                 * access$200:419, AbstractChannel$AbstractUnsafe (io.netty.channel)
                 * run:478, AbstractChannel$AbstractUnsafe$1 (io.netty.channel)
                 * safeExecute$$$capture:163, AbstractEventExecutor (io.netty.util.concurrent)
                 * safeExecute:-1, AbstractEventExecutor (io.netty.util.concurrent)
                 *  - Async stack trace
                 * addTask:-1, SingleThreadEventExecutor (io.netty.util.concurrent)
                 * execute:761, SingleThreadEventExecutor (io.netty.util.concurrent)
                 * register:475, AbstractChannel$AbstractUnsafe (io.netty.channel)
                 * register:80, SingleThreadEventLoop (io.netty.channel)
                 * register:74, SingleThreadEventLoop (io.netty.channel)
                 * register:85, MultithreadEventLoopGroup (io.netty.channel)
                 * channelRead:254, ServerBootstrap$ServerBootstrapAcceptor (io.netty.bootstrap)
                 * invokeChannelRead:373, AbstractChannelHandlerContext (io.netty.channel)
                 * invokeChannelRead:359, AbstractChannelHandlerContext (io.netty.channel)
                 * fireChannelRead:351, AbstractChannelHandlerContext (io.netty.channel)
                 * channelRead:86, ChannelInboundHandlerAdapter (io.netty.channel)
                 * channelRead:26, ServerHandler (com.imooc.netty.ch3)
                 * invokeChannelRead:373, AbstractChannelHandlerContext (io.netty.channel)
                 * invokeChannelRead:359, AbstractChannelHandlerContext (io.netty.channel)
                 * fireChannelRead:351, AbstractChannelHandlerContext (io.netty.channel)
                 * channelRead:1334, DefaultChannelPipeline$HeadContext (io.netty.channel)
                 * invokeChannelRead:373, AbstractChannelHandlerContext (io.netty.channel)
                 * invokeChannelRead:359, AbstractChannelHandlerContext (io.netty.channel)
                 * fireChannelRead:926, DefaultChannelPipeline (io.netty.channel)
                 * read:93, AbstractNioMessageChannel$NioMessageUnsafe (io.netty.channel.nio)
                 * processSelectedKey:651, NioEventLoop (io.netty.channel.nio)
                 * processSelectedKeysOptimized:574, NioEventLoop (io.netty.channel.nio)
                 * processSelectedKeys:488, NioEventLoop (io.netty.channel.nio)
                 * run:450, NioEventLoop (io.netty.channel.nio)
                 * run:873, SingleThreadEventExecutor$5 (io.netty.util.concurrent)
                 * run:144, DefaultThreadFactory$DefaultRunnableDecorator (io.netty.util.concurrent)
                 * run:748, Thread (java.lang)
                 */
                int size = readBuf.size();
                for (int i = 0; i < size; i ++) {
                    readPending = false;
                    pipeline.fireChannelRead(readBuf.get(i));
                }
                readBuf.clear();
                allocHandle.readComplete();
                pipeline.fireChannelReadComplete();

                if (exception != null) {
                    closed = closeOnReadError(exception);

                    pipeline.fireExceptionCaught(exception);
                }

                if (closed) {
                    inputShutdown = true;
                    if (isOpen()) {
                        close(voidPromise());
                    }
                }
            } finally {
                // Check if there is a readPending which was not processed yet.
                // This could be for two reasons:
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelRead(...) method
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelReadComplete(...) method
                //
                // See https://github.com/netty/netty/issues/2254
                if (!readPending && !config.isAutoRead()) {
                    removeReadOp();
                }
            }
        }
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        final SelectionKey key = selectionKey();
        final int interestOps = key.interestOps();

        for (;;) {
            Object msg = in.current();
            if (msg == null) {
                // Wrote all messages.
                if ((interestOps & SelectionKey.OP_WRITE) != 0) {
                    key.interestOps(interestOps & ~SelectionKey.OP_WRITE);
                }
                break;
            }
            try {
                boolean done = false;
                for (int i = config().getWriteSpinCount() - 1; i >= 0; i--) {
                    if (doWriteMessage(msg, in)) {
                        done = true;
                        break;
                    }
                }

                if (done) {
                    in.remove();
                } else {
                    // Did not write all messages.
                    if ((interestOps & SelectionKey.OP_WRITE) == 0) {
                        key.interestOps(interestOps | SelectionKey.OP_WRITE);
                    }
                    break;
                }
            } catch (IOException e) {
                if (continueOnWriteError()) {
                    in.remove(e);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Returns {@code true} if we should continue the write loop on a write error.
     */
    protected boolean continueOnWriteError() {
        return false;
    }

    protected boolean closeOnReadError(Throwable cause) {
        // ServerChannel should not be closed even on IOException because it can often continue
        // accepting incoming connections. (e.g. too many open files)
        return cause instanceof IOException &&
                !(cause instanceof PortUnreachableException) &&
                this instanceof ServerChannel;
    }

    /**
     * Read messages into the given array and return the amount which was read.
     */
    protected abstract int doReadMessages(List<Object> buf) throws Exception;

    /**
     * Write a message to the underlying {@link java.nio.channels.Channel}.
     *
     * @return {@code true} if and only if the message has been written
     */
    protected abstract boolean doWriteMessage(Object msg, ChannelOutboundBuffer in) throws Exception;
}
