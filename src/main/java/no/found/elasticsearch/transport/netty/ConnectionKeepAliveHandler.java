/*
 * This is the MIT license: http://www.opensource.org/licenses/mit-license.php
 *
 * Copyright (c) 2010-2012, Found IT A/S.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
 * to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
 * FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
package no.found.elasticsearch.transport.netty;

import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBuffers;
import org.elasticsearch.common.netty.channel.ChannelHandlerContext;
import org.elasticsearch.common.netty.channel.Channels;
import org.elasticsearch.common.netty.channel.DownstreamMessageEvent;
import org.elasticsearch.common.netty.channel.LifeCycleAwareChannelHandler;
import org.elasticsearch.common.netty.channel.MessageEvent;
import org.elasticsearch.common.netty.channel.SimpleChannelHandler;
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ConnectionKeepAliveHandler extends SimpleChannelHandler implements LifeCycleAwareChannelHandler {
    private final ScheduledExecutorService scheduler;
    private final TimeValue keepAliveInterval;
    ChannelBuffer keepAliveBuffer = ChannelBuffers.copiedBuffer(new byte[]{'F', 'K', 0, 0, 0, 0});
    private ScheduledFuture<?> currentScheduled;

    public ConnectionKeepAliveHandler(ScheduledExecutorService scheduler, TimeValue keepAliveInterval) {
        this.scheduler = scheduler;
        this.keepAliveInterval = keepAliveInterval;
    }


    private void addTimeoutTask(ChannelHandlerContext ctx) {
        cancelCurrentScheduled();
        currentScheduled = scheduler.schedule(new KeepAliveRunnable(ctx), 2, TimeUnit.SECONDS);
    }

    private void cancelCurrentScheduled() {
        if (currentScheduled != null && !currentScheduled.isCancelled() && !currentScheduled.isDone()) {
            currentScheduled.cancel(true);
        }
    }

    private long lastWrite;

    @Override
    synchronized public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        super.writeRequested(ctx, e);
        lastWrite = System.currentTimeMillis();
    }

    @Override
    public void beforeAdd(ChannelHandlerContext channelHandlerContext) throws Exception {
    }

    @Override
    synchronized public void afterAdd(ChannelHandlerContext channelHandlerContext) throws Exception {
        lastWrite = System.currentTimeMillis();
        addTimeoutTask(channelHandlerContext);
    }

    @Override
    public void beforeRemove(ChannelHandlerContext channelHandlerContext) throws Exception {
    }

    @Override
    public void afterRemove(ChannelHandlerContext channelHandlerContext) throws Exception {
        cancelCurrentScheduled();
    }

    class KeepAliveRunnable implements Runnable {
        private final ChannelHandlerContext ctx;

        public KeepAliveRunnable(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!ctx.getChannel().isConnected()) {
                return;
            }

            ctx.getPipeline().execute(new Runnable() {
                @Override
                public void run() {
                    send(ctx, new DownstreamMessageEvent(ctx.getChannel(), Channels.future(ctx.getChannel()), keepAliveBuffer, ctx.getChannel().getRemoteAddress()));
                    addTimeoutTask(ctx);
                }
            });
        }
    }

    synchronized private void send(ChannelHandlerContext ctx, DownstreamMessageEvent downstreamMessageEvent) {
        long now = System.currentTimeMillis();

        if (now - lastWrite > keepAliveInterval.millis()) {
            lastWrite = now;
            ctx.sendDownstream(downstreamMessageEvent);
        }
    }
}
