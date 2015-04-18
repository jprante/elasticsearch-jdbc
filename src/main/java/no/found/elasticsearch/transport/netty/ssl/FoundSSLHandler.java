/*
 * Copyright 2009 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package no.found.elasticsearch.transport.netty.ssl;

import no.found.elasticsearch.transport.netty.ssl.internal.NonReentrantLock;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBuffers;
import org.elasticsearch.common.netty.channel.Channel;
import org.elasticsearch.common.netty.channel.ChannelDownstreamHandler;
import org.elasticsearch.common.netty.channel.ChannelEvent;
import org.elasticsearch.common.netty.channel.ChannelFuture;
import org.elasticsearch.common.netty.channel.ChannelFutureListener;
import org.elasticsearch.common.netty.channel.ChannelHandlerContext;
import org.elasticsearch.common.netty.channel.ChannelStateEvent;
import org.elasticsearch.common.netty.channel.Channels;
import org.elasticsearch.common.netty.channel.DefaultChannelFuture;
import org.elasticsearch.common.netty.channel.DownstreamMessageEvent;
import org.elasticsearch.common.netty.channel.ExceptionEvent;
import org.elasticsearch.common.netty.channel.MessageEvent;
import org.elasticsearch.common.netty.handler.codec.frame.FrameDecoder;
import org.elasticsearch.common.netty.logging.InternalLogger;
import org.elasticsearch.common.netty.logging.InternalLoggerFactory;
import org.elasticsearch.common.netty.util.Timeout;
import org.elasticsearch.common.netty.util.Timer;
import org.elasticsearch.common.netty.util.TimerTask;
import org.elasticsearch.common.netty.util.internal.DetectionUtil;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static org.elasticsearch.common.netty.channel.Channels.failedFuture;
import static org.elasticsearch.common.netty.channel.Channels.fireExceptionCaught;
import static org.elasticsearch.common.netty.channel.Channels.fireExceptionCaughtLater;
import static org.elasticsearch.common.netty.channel.Channels.future;
import static org.elasticsearch.common.netty.channel.Channels.succeededFuture;
import static org.elasticsearch.common.netty.channel.Channels.write;

/**
 * Adds <a href="http://en.wikipedia.org/wiki/Transport_Layer_Security">SSL
 * &middot; TLS</a> and StartTLS support to a {@link org.elasticsearch.common.netty.channel.Channel}.  Please refer
 * to the <strong>"SecureChat"</strong> example in the distribution or the web
 * site for the detailed usage.
 * <h3>Beginning the handshake</h3>
 * You must make sure not to write a message while the
 * {@linkplain #handshake() handshake} is in progress unless you are
 * renegotiating.  You will be notified by the {@link org.elasticsearch.common.netty.channel.ChannelFuture} which is
 * returned by the {@link #handshake()} method when the handshake
 * process succeeds or fails.
 * <h3>Handshake</h3>
 * If {@link #isIssueHandshake()} is {@code false}
 * (default) you will need to take care of calling {@link #handshake()} by your own. In most
 * situations were {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler} is used in 'client mode' you want to issue a handshake once
 * the connection was established. if {@link #setIssueHandshake(boolean)} is set to {@code true}
 * you don't need to worry about this as the {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler} will take care of it.
 * <h3>Renegotiation</h3>
 * If {@link #isEnableRenegotiation() enableRenegotiation} is {@code true}
 * (default) and the initial handshake has been done successfully, you can call
 * {@link #handshake()} to trigger the renegotiation.
 * If {@link #isEnableRenegotiation() enableRenegotiation} is {@code false},
 * an attempt to trigger renegotiation will result in the connection closure.
 * Please note that TLS renegotiation had a security issue before.  If your
 * runtime environment did not fix it, please make sure to disable TLS
 * renegotiation by calling {@link #setEnableRenegotiation(boolean)} with
 * {@code false}.  For more information, please refer to the following documents:
 * <ul>
 * <li><a href="http://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2009-3555">CVE-2009-3555</a></li>
 * <li><a href="http://www.ietf.org/rfc/rfc5746.txt">RFC5746</a></li>
 * <li><a href="http://www.oracle.com/technetwork/java/javase/documentation/tlsreadme2-176330.html">Phased
 * Approach to Fixing the TLS Renegotiation Issue</a></li>
 * </ul>
 * <h3>Closing the session</h3>
 * To close the SSL session, the {@link #close()} method should be
 * called to send the {@code close_notify} message to the remote peer.  One
 * exception is when you close the {@link org.elasticsearch.common.netty.channel.Channel} - {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler}
 * intercepts the close request and send the {@code close_notify} message
 * before the channel closure automatically.  Once the SSL session is closed,
 * it is not reusable, and consequently you should create a new
 * {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler} with a new {@link javax.net.ssl.SSLEngine} as explained in the
 * following section.
 * <h3>Restarting the session</h3>
 * To restart the SSL session, you must remove the existing closed
 * {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler} from the {@link org.elasticsearch.common.netty.channel.ChannelPipeline}, insert a new
 * {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler} with a new {@link javax.net.ssl.SSLEngine} into the pipeline,
 * and start the handshake process as described in the first section.
 * <h3>Implementing StartTLS</h3>
 * <a href="http://en.wikipedia.org/wiki/STARTTLS">StartTLS</a> is the
 * communication pattern that secures the wire in the middle of the plaintext
 * connection.  Please note that it is different from SSL &middot; TLS, that
 * secures the wire from the beginning of the connection.  Typically, StartTLS
 * is composed of three steps:
 * <ol>
 * <li>Client sends a StartTLS request to server.</li>
 * <li>Server sends a StartTLS response to client.</li>
 * <li>Client begins SSL handshake.</li>
 * </ol>
 * If you implement a server, you need to:
 * <ol>
 * <li>create a new {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler} instance with {@code startTls} flag set
 * to {@code true},</li>
 * <li>insert the {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler} to the {@link org.elasticsearch.common.netty.channel.ChannelPipeline}, and</li>
 * <li>write a StartTLS response.</li>
 * </ol>
 * Please note that you must insert {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler} <em>before</em> sending
 * the StartTLS response.  Otherwise the client can send begin SSL handshake
 * before {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler} is inserted to the {@link org.elasticsearch.common.netty.channel.ChannelPipeline}, causing
 * data corruption.
 * The client-side implementation is much simpler.
 * <ol>
 * <li>Write a StartTLS request,</li>
 * <li>wait for the StartTLS response,</li>
 * <li>create a new {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler} instance with {@code startTls} flag set
 * to {@code false},</li>
 * <li>insert the {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler} to the {@link org.elasticsearch.common.netty.channel.ChannelPipeline}, and</li>
 * <li>Initiate SSL handshake by calling {@link no.found.elasticsearch.transport.netty.ssl.FoundSSLHandler#handshake()}.</li>
 * </ol>
 * <h3>Known issues</h3>
 * Because of a known issue with the current implementation of the SslEngine that comes
 * with Java it may be possible that you see blocked IO-Threads while a full GC is done.
 * So if you are affected you can workaround this problem by adjust the cache settings
 * like shown below:
 * <pre>
 *     SslContext context = ...;
 *     context.getServerSessionContext().setSessionCacheSize(someSaneSize);
 *     context.getServerSessionContext().setSessionTime(someSameTimeout);
 * </pre>
 * What values to use here depends on the nature of your application and should be set
 * based on monitoring and debugging of it.
 * For more details see
 * <a href="https://github.com/netty/netty/issues/832">#832</a> in our issue tracker.
 *
 */
public class FoundSSLHandler extends FrameDecoder implements ChannelDownstreamHandler {

    private static final InternalLogger logger =
            InternalLoggerFactory.getInstance(FoundSSLHandler.class);

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private static final Pattern IGNORABLE_CLASS_IN_STACK = Pattern.compile(
            "^.*(?:Socket|Datagram|Sctp|Udt)Channel.*$");
    private static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
            "^.*(?:connection.*(?:reset|closed|abort|broken)|broken.*pipe).*$", Pattern.CASE_INSENSITIVE);

    private static SslBufferPool defaultBufferPool;

    /**
     * Returns the default {@link no.found.elasticsearch.transport.netty.ssl.SslBufferPool} used when no pool is
     * specified in the constructor.
     */
    public static synchronized SslBufferPool getDefaultBufferPool() {
        if (defaultBufferPool == null) {
            defaultBufferPool = new SslBufferPool();
        }
        return defaultBufferPool;
    }

    private volatile ChannelHandlerContext ctx;
    private final SSLEngine engine;
    private final SslBufferPool bufferPool;
    private final Executor delegatedTaskExecutor;
    private final boolean startTls;

    private volatile boolean enableRenegotiation = true;

    final Object handshakeLock = new Object();
    private boolean handshaking;
    private volatile boolean handshaken;
    private volatile ChannelFuture handshakeFuture;

    private final AtomicBoolean sentFirstMessage = new AtomicBoolean();
    private final AtomicBoolean sentCloseNotify = new AtomicBoolean();
    int ignoreClosedChannelException;
    final Object ignoreClosedChannelExceptionLock = new Object();
    private final Queue<PendingWrite> pendingUnencryptedWrites = new LinkedList<PendingWrite>();
    private final NonReentrantLock pendingUnencryptedWritesLock = new NonReentrantLock();
    private final Queue<MessageEvent> pendingEncryptedWrites = new ConcurrentLinkedQueue<MessageEvent>();
    private final NonReentrantLock pendingEncryptedWritesLock = new NonReentrantLock();

    private volatile boolean issueHandshake;

    private final SSLEngineInboundCloseFuture sslEngineCloseFuture = new SSLEngineInboundCloseFuture();

    private boolean closeOnSSLException;

    private int packetLength = Integer.MIN_VALUE;

    private final Timer timer;
    private final long handshakeTimeoutInMillis;
    private Timeout handshakeTimeout;

    /**
     * Creates a new instance.
     *
     * @param engine the {@link javax.net.ssl.SSLEngine} this handler will use
     */
    public FoundSSLHandler(SSLEngine engine) {
        this(engine, getDefaultBufferPool(), ImmediateExecutor.INSTANCE);
    }

    /**
     * Creates a new instance.
     *
     * @param engine     the {@link javax.net.ssl.SSLEngine} this handler will use
     * @param bufferPool the {@link no.found.elasticsearch.transport.netty.ssl.SslBufferPool} where this handler will
     *                   acquire the buffers required by the {@link javax.net.ssl.SSLEngine}
     */
    public FoundSSLHandler(SSLEngine engine, SslBufferPool bufferPool) {
        this(engine, bufferPool, ImmediateExecutor.INSTANCE);
    }

    /**
     * Creates a new instance.
     *
     * @param engine   the {@link javax.net.ssl.SSLEngine} this handler will use
     * @param startTls {@code true} if the first write request shouldn't be
     *                 encrypted by the {@link javax.net.ssl.SSLEngine}
     */
    public FoundSSLHandler(SSLEngine engine, boolean startTls) {
        this(engine, getDefaultBufferPool(), startTls);
    }

    /**
     * Creates a new instance.
     *
     * @param engine     the {@link javax.net.ssl.SSLEngine} this handler will use
     * @param bufferPool the {@link no.found.elasticsearch.transport.netty.ssl.SslBufferPool} where this handler will
     *                   acquire the buffers required by the {@link javax.net.ssl.SSLEngine}
     * @param startTls   {@code true} if the first write request shouldn't be
     *                   encrypted by the {@link javax.net.ssl.SSLEngine}
     */
    public FoundSSLHandler(SSLEngine engine, SslBufferPool bufferPool, boolean startTls) {
        this(engine, bufferPool, startTls, ImmediateExecutor.INSTANCE);
    }

    /**
     * Creates a new instance.
     *
     * @param engine                the {@link javax.net.ssl.SSLEngine} this handler will use
     * @param delegatedTaskExecutor the {@link java.util.concurrent.Executor} which will execute the delegated task
     *                              that {@link javax.net.ssl.SSLEngine#getDelegatedTask()} will return
     */
    public FoundSSLHandler(SSLEngine engine, Executor delegatedTaskExecutor) {
        this(engine, getDefaultBufferPool(), delegatedTaskExecutor);
    }

    /**
     * Creates a new instance.
     *
     * @param engine                the {@link javax.net.ssl.SSLEngine} this handler will use
     * @param bufferPool            the {@link no.found.elasticsearch.transport.netty.ssl.SslBufferPool} where this handler will acquire
     *                              the buffers required by the {@link javax.net.ssl.SSLEngine}
     * @param delegatedTaskExecutor the {@link java.util.concurrent.Executor} which will execute the delegated task
     *                              that {@link javax.net.ssl.SSLEngine#getDelegatedTask()} will return
     */
    public FoundSSLHandler(SSLEngine engine, SslBufferPool bufferPool, Executor delegatedTaskExecutor) {
        this(engine, bufferPool, false, delegatedTaskExecutor);
    }

    /**
     * Creates a new instance.
     *
     * @param engine                the {@link javax.net.ssl.SSLEngine} this handler will use
     * @param startTls              {@code true} if the first write request shouldn't be encrypted
     *                              by the {@link javax.net.ssl.SSLEngine}
     * @param delegatedTaskExecutor the {@link java.util.concurrent.Executor} which will execute the delegated task
     *                              that {@link javax.net.ssl.SSLEngine#getDelegatedTask()} will return
     */
    public FoundSSLHandler(SSLEngine engine, boolean startTls, Executor delegatedTaskExecutor) {
        this(engine, getDefaultBufferPool(), startTls, delegatedTaskExecutor);
    }

    /**
     * Creates a new instance.
     *
     * @param engine                the {@link javax.net.ssl.SSLEngine} this handler will use
     * @param bufferPool            the {@link no.found.elasticsearch.transport.netty.ssl.SslBufferPool} where this handler will acquire
     *                              the buffers required by the {@link javax.net.ssl.SSLEngine}
     * @param startTls              {@code true} if the first write request shouldn't be encrypted
     *                              by the {@link javax.net.ssl.SSLEngine}
     * @param delegatedTaskExecutor the {@link java.util.concurrent.Executor} which will execute the delegated task
     *                              that {@link javax.net.ssl.SSLEngine#getDelegatedTask()} will return
     */
    public FoundSSLHandler(SSLEngine engine, SslBufferPool bufferPool, boolean startTls, Executor delegatedTaskExecutor) {
        this(engine, bufferPool, startTls, delegatedTaskExecutor, null, 0);
    }

    /**
     * Creates a new instance.
     *
     * @param engine                   the {@link javax.net.ssl.SSLEngine} this handler will use
     * @param bufferPool               the {@link no.found.elasticsearch.transport.netty.ssl.SslBufferPool} where this handler will acquire
     *                                 the buffers required by the {@link javax.net.ssl.SSLEngine}
     * @param startTls                 {@code true} if the first write request shouldn't be encrypted
     *                                 by the {@link javax.net.ssl.SSLEngine}
     * @param delegatedTaskExecutor    the {@link java.util.concurrent.Executor} which will execute the delegated task
     *                                 that {@link javax.net.ssl.SSLEngine#getDelegatedTask()} will return
     * @param timer                    the {@link org.elasticsearch.common.netty.util.Timer} which will be used to process the timeout of the {@link #handshake()}.
     *                                 Be aware that the given {@link org.elasticsearch.common.netty.util.Timer} will not get stopped automaticly, so it is up to you to cleanup
     *                                 once you not need it anymore
     * @param handshakeTimeoutInMillis the time in milliseconds after whic the {@link #handshake()}  will be failed, and so the future notified
     */
    public FoundSSLHandler(SSLEngine engine, SslBufferPool bufferPool, boolean startTls, Executor delegatedTaskExecutor,
                           Timer timer, long handshakeTimeoutInMillis) {
        if (engine == null) {
            throw new NullPointerException("engine");
        }
        if (bufferPool == null) {
            throw new NullPointerException("bufferPool");
        }
        if (delegatedTaskExecutor == null) {
            throw new NullPointerException("delegatedTaskExecutor");
        }
        if (timer == null && handshakeTimeoutInMillis > 0) {
            throw new IllegalArgumentException("No Timer was given but a handshakeTimeoutInMillis, need both or none");
        }

        this.engine = engine;
        this.bufferPool = bufferPool;
        this.delegatedTaskExecutor = delegatedTaskExecutor;
        this.startTls = startTls;
        this.timer = timer;
        this.handshakeTimeoutInMillis = handshakeTimeoutInMillis;
    }

    /**
     * Returns the {@link javax.net.ssl.SSLEngine} which is used by this handler.
     */
    public SSLEngine getEngine() {
        return engine;
    }

    /**
     * Starts an SSL / TLS handshake for the specified channel.
     *
     * @return a {@link org.elasticsearch.common.netty.channel.ChannelFuture} which is notified when the handshake
     * succeeds or fails.
     */
    public ChannelFuture handshake() {
        synchronized (handshakeLock) {
            if (handshaken && !isEnableRenegotiation()) {
                throw new IllegalStateException("renegotiation disabled");
            }

            final ChannelHandlerContext ctx = this.ctx;
            final Channel channel = ctx.getChannel();
            ChannelFuture handshakeFuture;
            Exception exception = null;

            if (handshaking) {
                return this.handshakeFuture;
            }

            handshaking = true;
            try {
                engine.beginHandshake();
                runDelegatedTasks();
                handshakeFuture = this.handshakeFuture = future(channel);
                if (handshakeTimeoutInMillis > 0) {
                    handshakeTimeout = timer.newTimeout(new TimerTask() {
                        public void run(Timeout timeout) throws Exception {
                            ChannelFuture future = FoundSSLHandler.this.handshakeFuture;
                            if (future != null && future.isDone()) {
                                return;
                            }

                            setHandshakeFailure(channel, new SSLException("Handshake did not complete within " +
                                    handshakeTimeoutInMillis + "ms"));
                        }
                    }, handshakeTimeoutInMillis, TimeUnit.MILLISECONDS);
                }
            } catch (Exception e) {
                handshakeFuture = this.handshakeFuture = failedFuture(channel, e);
                exception = e;
            }

            if (exception == null) { // Began handshake successfully.
                try {
                    final ChannelFuture hsFuture = handshakeFuture;
                    wrapNonAppData(ctx, channel).addListener(new ChannelFutureListener() {
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (!future.isSuccess()) {
                                Throwable cause = future.getCause();
                                hsFuture.setFailure(cause);

                                fireExceptionCaught(ctx, cause);
                                if (closeOnSSLException) {
                                    Channels.close(ctx, future(channel));
                                }
                            }
                        }
                    });
                } catch (SSLException e) {
                    handshakeFuture.setFailure(e);

                    fireExceptionCaught(ctx, e);
                    if (closeOnSSLException) {
                        Channels.close(ctx, future(channel));
                    }
                }
            } else { // Failed to initiate handshake.
                fireExceptionCaught(ctx, exception);
                if (closeOnSSLException) {
                    Channels.close(ctx, future(channel));
                }
            }
            return handshakeFuture;
        }
    }

    /**
     * @deprecated Use {@link #handshake()} instead.
     */
    @Deprecated
    public ChannelFuture handshake(@SuppressWarnings("unused") Channel channel) {
        return handshake();
    }

    /**
     * Sends an SSL {@code close_notify} message to the specified channel and
     * destroys the underlying {@link javax.net.ssl.SSLEngine}.
     */
    public ChannelFuture close() {
        ChannelHandlerContext ctx = this.ctx;
        Channel channel = ctx.getChannel();
        try {
            engine.closeOutbound();
            return wrapNonAppData(ctx, channel);
        } catch (SSLException e) {
            fireExceptionCaught(ctx, e);
            if (closeOnSSLException) {
                Channels.close(ctx, future(channel));
            }
            return failedFuture(channel, e);
        }
    }

    /**
     * @deprecated Use {@link #close()} instead.
     */
    @Deprecated
    public ChannelFuture close(@SuppressWarnings("unused") Channel channel) {
        return close();
    }

    /**
     * Returns {@code true} if and only if TLS renegotiation is enabled.
     */
    public boolean isEnableRenegotiation() {
        return enableRenegotiation;
    }

    /**
     * Enables or disables TLS renegotiation.
     */
    public void setEnableRenegotiation(boolean enableRenegotiation) {
        this.enableRenegotiation = enableRenegotiation;
    }

    /**
     * Enables or disables the automatic handshake once the {@link org.elasticsearch.common.netty.channel.Channel} is
     * connected. The value will only have affect if its set before the
     * {@link org.elasticsearch.common.netty.channel.Channel} is connected.
     */
    public void setIssueHandshake(boolean issueHandshake) {
        this.issueHandshake = issueHandshake;
    }

    /**
     * Returns {@code true} if the automatic handshake is enabled
     */
    public boolean isIssueHandshake() {
        return issueHandshake;
    }

    /**
     * Return the {@link org.elasticsearch.common.netty.channel.ChannelFuture} that will get notified if the inbound of the {@link javax.net.ssl.SSLEngine} will get closed.
     * <p/>
     * This method will return the same {@link org.elasticsearch.common.netty.channel.ChannelFuture} all the time.
     * <p/>
     * For more informations see the apidocs of {@link javax.net.ssl.SSLEngine}
     */
    public ChannelFuture getSSLEngineInboundCloseFuture() {
        return sslEngineCloseFuture;
    }

    /**
     * Return the timeout (in ms) after which the {@link org.elasticsearch.common.netty.channel.ChannelFuture} of {@link #handshake()} will be failed, while
     * a handshake is in progress
     */
    public long getHandshakeTimeout() {
        return handshakeTimeoutInMillis;
    }

    /**
     * If set to {@code true}, the {@link org.elasticsearch.common.netty.channel.Channel} will automatically get closed
     * one a {@link javax.net.ssl.SSLException} was caught. This is most times what you want, as after this
     * its almost impossible to recover.
     * <p/>
     * Anyway the default is {@code false} to not break compatibility with older releases. This
     * will be changed to {@code true} in the next major release.
     */
    public void setCloseOnSSLException(boolean closeOnSslException) {
        if (ctx != null) {
            throw new IllegalStateException("Can only get changed before attached to ChannelPipeline");
        }
        closeOnSSLException = closeOnSslException;
    }

    public boolean getCloseOnSSLException() {
        return closeOnSSLException;
    }

    public void handleDownstream(
            final ChannelHandlerContext context, final ChannelEvent evt) throws Exception {
        if (evt instanceof ChannelStateEvent) {
            ChannelStateEvent e = (ChannelStateEvent) evt;
            switch (e.getState()) {
                case OPEN:
                case CONNECTED:
                case BOUND:
                    if (Boolean.FALSE.equals(e.getValue()) || e.getValue() == null) {
                        //closeOutboundAndChannel(context, e);
                        Channels.close(context, e.getFuture());
                        return;
                    }
            }
        }
        if (!(evt instanceof MessageEvent)) {
            context.sendDownstream(evt);
            return;
        }

        MessageEvent e = (MessageEvent) evt;
        if (!(e.getMessage() instanceof ChannelBuffer)) {
            context.sendDownstream(evt);
            return;
        }

        // Do not encrypt the first write request if this handler is
        // created with startTLS flag turned on.
        if (startTls && sentFirstMessage.compareAndSet(false, true)) {
            context.sendDownstream(evt);
            return;
        }

        // Otherwise, all messages are encrypted.
        ChannelBuffer msg = (ChannelBuffer) e.getMessage();
        PendingWrite pendingWrite;

        if (msg.readable()) {
            pendingWrite = new PendingWrite(evt.getFuture(), msg.toByteBuffer(msg.readerIndex(), msg.readableBytes()));
        } else {
            pendingWrite = new PendingWrite(evt.getFuture(), null);
        }

        pendingUnencryptedWritesLock.lock();
        try {
            pendingUnencryptedWrites.add(pendingWrite);
        } finally {
            pendingUnencryptedWritesLock.unlock();
        }

        wrap(context, evt.getChannel());
    }

    private void cancelHandshakeTimeout() {
        if (handshakeTimeout != null) {
            // cancel the task as we will fail the handshake future now
            handshakeTimeout.cancel();
        }
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx,
                                    ChannelStateEvent e) throws Exception {

        // Make sure the handshake future is notified when a connection has
        // been closed during handshake.
        synchronized (handshakeLock) {
            if (handshaking) {
                cancelHandshakeTimeout();
                handshakeFuture.setFailure(new ClosedChannelException());
            }
        }

        try {
            super.channelDisconnected(ctx, e);
        } finally {
            try {
                unwrap(ctx, e.getChannel(), ChannelBuffers.EMPTY_BUFFER, 0, 0);
            } catch (Exception cce) {
                // TODO: ignore?
            }
            engine.closeOutbound();
            if (!sentCloseNotify.get() && handshaken) {
                try {
                    engine.closeInbound();
                } catch (SSLException ex) {
                    if (logger.isDebugEnabled() && !ex.getMessage().contains("Inbound closed before receiving peer's close_notify")) {
                        logger.debug("Failed to clean up SSLEngine.", ex);
                    }
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {

        if (true) {
            return;
        }

        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
            if (cause instanceof ClosedChannelException) {
                synchronized (ignoreClosedChannelExceptionLock) {
                    if (ignoreClosedChannelException > 0) {
                        ignoreClosedChannelException--;
                        if (logger.isDebugEnabled()) {
                            return;
                            /*
                            logger.debug(
                                    "Swallowing an exception raised while " +
                                            "writing non-app data", cause);
                                            */
                        }

                        return;
                    }
                }
            } else {
                if (ignoreException(cause)) {
                    return;
                }
            }
        }

        if (cause instanceof SSLException && (cause.getMessage().contains("closing") || cause.getMessage().contains("closed"))) {
            return;
        }

        ctx.sendUpstream(e);
    }

    /**
     * Checks if the given {@link Throwable} can be ignore and just "swallowed"
     * <p/>
     * When an ssl connection is closed a close_notify message is sent.
     * After that the peer also sends close_notify however, it's not mandatory to receive
     * the close_notify. The party who sent the initial close_notify can close the connection immediately
     * then the peer will get connection reset error.
     */
    private boolean ignoreException(Throwable t) {
        if (!(t instanceof SSLException) && t instanceof IOException && engine.isOutboundDone()) {
            String message = String.valueOf(t.getMessage()).toLowerCase();

            // first try to match connection reset / broke peer based on the regex. This is the fastest way
            // but may fail on different jdk impls or OS's
            if (IGNORABLE_ERROR_MESSAGE.matcher(message).matches()) {
                return true;
            }

            // Inspect the StackTraceElements to see if it was a connection reset / broken pipe or not
            StackTraceElement[] elements = t.getStackTrace();
            for (StackTraceElement element : elements) {
                String classname = element.getClassName();
                String methodname = element.getMethodName();

                // skip all classes that belong to the io.netty package
                if (classname.startsWith("org.jboss.netty.")) {
                    continue;
                }

                // check if the method name is read if not skip it
                if (!"read".equals(methodname)) {
                    continue;
                }

                // This will also match against SocketInputStream which is used by openjdk 7 and maybe
                // also others
                if (IGNORABLE_CLASS_IN_STACK.matcher(classname).matches()) {
                    return true;
                }

                try {
                    // No match by now.. Try to load the class via classloader and inspect it.
                    // This is mainly done as other JDK implementations may differ in name of
                    // the impl.
                    Class<?> clazz = getClass().getClassLoader().loadClass(classname);

                    if (SocketChannel.class.isAssignableFrom(clazz)
                            || DatagramChannel.class.isAssignableFrom(clazz)) {
                        return true;
                    }

                    // also match against SctpChannel via String matching as it may not present.
                    if (DetectionUtil.javaVersion() >= 7
                            && "com.sun.nio.sctp.SctpChannel".equals(clazz.getSuperclass().getName())) {
                        return true;
                    }
                } catch (ClassNotFoundException e) {
                    // This should not happen just ignore
                }
            }
        }

        return false;
    }

    /**
     * Returns {@code true} if the given {@link org.elasticsearch.common.netty.buffer.ChannelBuffer} is encrypted. Be aware that this method
     * will not increase the readerIndex of the given {@link org.elasticsearch.common.netty.buffer.ChannelBuffer}.
     *
     * @param buffer The {@link org.elasticsearch.common.netty.buffer.ChannelBuffer} to read from. Be aware that it must have at least 5 bytes to read,
     *               otherwise it will throw an {@link IllegalArgumentException}.
     * @return encrypted
     * {@code true} if the {@link org.elasticsearch.common.netty.buffer.ChannelBuffer} is encrypted, {@code false} otherwise.
     * @throws IllegalArgumentException Is thrown if the given {@link org.elasticsearch.common.netty.buffer.ChannelBuffer} has not at least 5 bytes to read.
     */
    public static boolean isEncrypted(ChannelBuffer buffer) {
        return getEncryptedPacketLength(buffer) != -1;
    }

    /**
     * Return how much bytes can be read out of the encrypted data. Be aware that this method will not increase
     * the readerIndex of the given {@link org.elasticsearch.common.netty.buffer.ChannelBuffer}.
     *
     * @param buffer The {@link org.elasticsearch.common.netty.buffer.ChannelBuffer} to read from. Be aware that it must have at least 5 bytes to read,
     *               otherwise it will throw an {@link IllegalArgumentException}.
     * @return length
     * The length of the encrypted packet that is included in the buffer. This will
     * return {@code -1} if the given {@link org.elasticsearch.common.netty.buffer.ChannelBuffer} is not encrypted at all.
     * @throws IllegalArgumentException Is thrown if the given {@link org.elasticsearch.common.netty.buffer.ChannelBuffer} has not at least 5 bytes to read.
     */
    private static int getEncryptedPacketLength(ChannelBuffer buffer) {
        if (buffer.readableBytes() < 5) {
            throw new IllegalArgumentException("buffer must have at least 5 readable bytes");
        }

        int packetLength = 0;

        // SSLv3 or TLS - Check ContentType
        boolean tls;
        switch (buffer.getUnsignedByte(buffer.readerIndex())) {
            case 20:  // change_cipher_spec
            case 21:  // alert
            case 22:  // handshake
            case 23:  // application_data
                tls = true;
                break;
            default:
                // SSLv2 or bad data
                tls = false;
        }

        if (tls) {
            // SSLv3 or TLS - Check ProtocolVersion
            int majorVersion = buffer.getUnsignedByte(buffer.readerIndex() + 1);
            if (majorVersion == 3) {
                // SSLv3 or TLS
                packetLength = (getShort(buffer, buffer.readerIndex() + 3) & 0xFFFF) + 5;
                if (packetLength <= 5) {
                    // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
                    tls = false;
                }
            } else {
                // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
                tls = false;
            }
        }

        if (!tls) {
            // SSLv2 or bad data - Check the version
            boolean sslv2 = true;
            int headerLength = (buffer.getUnsignedByte(
                    buffer.readerIndex()) & 0x80) != 0 ? 2 : 3;
            int majorVersion = buffer.getUnsignedByte(
                    buffer.readerIndex() + headerLength + 1);
            if (majorVersion == 2 || majorVersion == 3) {
                // SSLv2
                if (headerLength == 2) {
                    packetLength = (getShort(buffer, buffer.readerIndex()) & 0x7FFF) + 2;
                } else {
                    packetLength = (getShort(buffer, buffer.readerIndex()) & 0x3FFF) + 3;
                }
                if (packetLength <= headerLength) {
                    sslv2 = false;
                }
            } else {
                sslv2 = false;
            }

            if (!sslv2) {
                return -1;
            }
        }
        return packetLength;
    }

    @Override
    protected Object decode(
            final ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {

        // Check if the packet length was parsed yet, if so we can skip the parsing
        if (packetLength == Integer.MIN_VALUE) {
            if (buffer.readableBytes() < 5) {
                return null;
            }
            int packetLength = getEncryptedPacketLength(buffer);

            if (packetLength == -1) {
                // Bad data - discard the buffer and raise an exception.
                NotSslRecordException e = new NotSslRecordException(
                        "not an SSL/TLS record: " + ChannelBuffers.hexDump(buffer));
                buffer.skipBytes(buffer.readableBytes());

                if (closeOnSSLException) {
                    // first trigger the exception and then close the channel
                    fireExceptionCaught(ctx, e);
                    Channels.close(ctx, future(channel));

                    // just return null as we closed the channel before, that
                    // will take care of cleanup etc
                    return null;
                } else {
                    throw e;
                }
            }

            assert packetLength > 0;
            this.packetLength = packetLength;
        }

        if (buffer.readableBytes() < packetLength) {
            return null;
        }

        // We advance the buffer's readerIndex before calling unwrap() because
        // unwrap() can trigger FrameDecoder call decode(), this method, recursively.
        // The recursive call results in decoding the same packet twice if
        // the readerIndex is advanced *after* decode().
        //
        // Here's an example:
        // 1) An SSL packet is received from the wire.
        // 2) FoundSSLHandler.decode() deciphers the packet and calls the user code.
        // 3) The user closes the channel in the same thread.
        // 4) The same thread triggers a channelDisconnected() event.
        // 5) FrameDecoder.cleanup() is called, and it calls FoundSSLHandler.decode().
        // 6) FoundSSLHandler.decode() will feed the same packet with what was
        //    deciphered at the step 2 again if the readerIndex was not advanced
        //    before calling the user code.
        final int packetOffset = buffer.readerIndex();
        buffer.skipBytes(packetLength);
        try {
            return unwrap(ctx, channel, buffer, packetOffset, packetLength);
        } finally {
            // reset the packet length so it will be parsed again on the next call
            packetLength = Integer.MIN_VALUE;
        }
    }

    /**
     * Reads a big-endian short integer from the buffer.  Please note that we do not use
     * {@link org.elasticsearch.common.netty.buffer.ChannelBuffer#getShort(int)} because it might be a little-endian buffer.
     */
    private static short getShort(ChannelBuffer buf, int offset) {
        return (short) (buf.getByte(offset) << 8 | buf.getByte(offset + 1) & 0xFF);
    }

    private void wrap(ChannelHandlerContext context, Channel channel)
            throws SSLException {

        ChannelBuffer msg;
        ByteBuffer outNetBuf = bufferPool.acquireBuffer();
        boolean success = true;
        boolean offered = false;
        boolean needsUnwrap = false;
        PendingWrite pendingWrite = null;

        try {
            loop:
            for (; ; ) {
                // Acquire a lock to make sure unencrypted data is polled
                // in order and their encrypted counterpart is offered in
                // order.
                pendingUnencryptedWritesLock.lock();
                try {
                    pendingWrite = pendingUnencryptedWrites.peek();
                    if (pendingWrite == null) {
                        break;
                    }

                    ByteBuffer outAppBuf = pendingWrite.outAppBuf;
                    if (outAppBuf == null) {
                        // A write request with an empty buffer
                        pendingUnencryptedWrites.remove();
                        offerEncryptedWriteRequest(
                                new DownstreamMessageEvent(
                                        channel, pendingWrite.future,
                                        ChannelBuffers.EMPTY_BUFFER,
                                        channel.getRemoteAddress()));
                        offered = true;
                    } else {
                        synchronized (handshakeLock) {
                            SSLEngineResult result = null;
                            try {
                                result = engine.wrap(outAppBuf, outNetBuf);
                            } finally {
                                if (!outAppBuf.hasRemaining()) {
                                    pendingUnencryptedWrites.remove();
                                }
                            }

                            if (result.bytesProduced() > 0) {
                                outNetBuf.flip();
                                int remaining = outNetBuf.remaining();
                                msg = ctx.getChannel().getConfig().getBufferFactory().getBuffer(remaining);

                                // Transfer the bytes to the new ChannelBuffer using some safe method that will also
                                // work with "non" heap buffers
                                //
                                // See https://github.com/netty/netty/issues/329
                                msg.writeBytes(outNetBuf);
                                outNetBuf.clear();

                                ChannelFuture future;
                                if (pendingWrite.outAppBuf.hasRemaining()) {
                                    // pendingWrite's future shouldn't be notified if
                                    // only partial data is written.
                                    future = succeededFuture(channel);
                                } else {
                                    future = pendingWrite.future;
                                }

                                MessageEvent encryptedWrite = new DownstreamMessageEvent(
                                        channel, future, msg, channel.getRemoteAddress());
                                offerEncryptedWriteRequest(encryptedWrite);
                                offered = true;
                            } else if (result.getStatus() == Status.CLOSED) {
                                // SSLEngine has been closed already.
                                // Any further write attempts should be denied.
                                success = false;
                                break;
                            } else {
                                final HandshakeStatus handshakeStatus = result.getHandshakeStatus();
                                handleRenegotiation(handshakeStatus);
                                switch (handshakeStatus) {
                                    case NEED_WRAP:
                                        if (outAppBuf.hasRemaining()) {
                                            break;
                                        } else {
                                            break loop;
                                        }
                                    case NEED_UNWRAP:
                                        needsUnwrap = true;
                                        break loop;
                                    case NEED_TASK:
                                        runDelegatedTasks();
                                        break;
                                    case FINISHED:
                                    case NOT_HANDSHAKING:
                                        if (handshakeStatus == HandshakeStatus.FINISHED) {
                                            setHandshakeSuccess(channel);
                                        }
                                        if (result.getStatus() == Status.CLOSED) {
                                            success = false;
                                        }
                                        break loop;
                                    default:
                                        throw new IllegalStateException(
                                                "Unknown handshake status: " +
                                                        handshakeStatus);
                                }
                            }
                        }
                    }
                } finally {
                    pendingUnencryptedWritesLock.unlock();
                }
            }
        } catch (SSLException e) {
            success = false;
            setHandshakeFailure(channel, e);
            throw e;
        } finally {
            bufferPool.releaseBuffer(outNetBuf);

            if (offered) {
                flushPendingEncryptedWrites(context);
            }

            if (!success) {
                IllegalStateException cause =
                        new IllegalStateException("SSLEngine already closed");

                // Check if we had a pendingWrite in process, if so we need to also notify as otherwise
                // the ChannelFuture will never get notified
                if (pendingWrite != null) {
                    pendingWrite.future.setFailure(cause);
                }

                // Mark all remaining pending writes as failure if anything
                // wrong happened before the write requests are wrapped.
                // Please note that we do not call setFailure while a lock is
                // acquired, to avoid a potential dead lock.
                for (; ; ) {
                    pendingUnencryptedWritesLock.lock();
                    try {
                        pendingWrite = pendingUnencryptedWrites.poll();
                        if (pendingWrite == null) {
                            break;
                        }
                    } finally {
                        pendingUnencryptedWritesLock.unlock();
                    }

                    pendingWrite.future.setFailure(cause);
                }
            }
        }

        if (needsUnwrap) {
            unwrap(context, channel, ChannelBuffers.EMPTY_BUFFER, 0, 0);
        }
    }

    private void offerEncryptedWriteRequest(MessageEvent encryptedWrite) {
        final boolean locked = pendingEncryptedWritesLock.tryLock();
        try {
            pendingEncryptedWrites.add(encryptedWrite);
        } finally {
            if (locked) {
                pendingEncryptedWritesLock.unlock();
            }
        }
    }

    private void flushPendingEncryptedWrites(ChannelHandlerContext ctx) {
        while (!pendingEncryptedWrites.isEmpty()) {
            // Avoid possible dead lock and data integrity issue
            // which is caused by cross communication between more than one channel
            // in the same VM.
            if (!pendingEncryptedWritesLock.tryLock()) {
                return;
            }

            try {
                MessageEvent e;
                while ((e = pendingEncryptedWrites.poll()) != null) {
                    ctx.sendDownstream(e);
                }
            } finally {
                pendingEncryptedWritesLock.unlock();
            }

            // Other thread might have added more elements at this point, so we loop again if the queue got unempty.
        }
    }

    private ChannelFuture wrapNonAppData(ChannelHandlerContext ctx, Channel channel) throws SSLException {
        ChannelFuture future = null;
        ByteBuffer outNetBuf = bufferPool.acquireBuffer();

        SSLEngineResult result;
        try {
            for (; ; ) {
                synchronized (handshakeLock) {
                    result = engine.wrap(EMPTY_BUFFER, outNetBuf);
                }

                if (result.bytesProduced() > 0) {
                    outNetBuf.flip();
                    ChannelBuffer msg =
                            ctx.getChannel().getConfig().getBufferFactory().getBuffer(outNetBuf.remaining());

                    // Transfer the bytes to the new ChannelBuffer using some safe method that will also
                    // work with "non" heap buffers
                    //
                    // See https://github.com/netty/netty/issues/329
                    msg.writeBytes(outNetBuf);
                    outNetBuf.clear();

                    future = future(channel);
                    future.addListener(new ChannelFutureListener() {
                        public void operationComplete(ChannelFuture future)
                                throws Exception {
                            if (future.getCause() instanceof ClosedChannelException) {
                                synchronized (ignoreClosedChannelExceptionLock) {
                                    ignoreClosedChannelException++;
                                }
                            }
                        }
                    });

                    write(ctx, future, msg);
                }

                final HandshakeStatus handshakeStatus = result.getHandshakeStatus();
                handleRenegotiation(handshakeStatus);
                switch (handshakeStatus) {
                    case FINISHED:
                        setHandshakeSuccess(channel);
                        runDelegatedTasks();
                        break;
                    case NEED_TASK:
                        runDelegatedTasks();
                        break;
                    case NEED_UNWRAP:
                        if (!Thread.holdsLock(handshakeLock)) {
                            // unwrap shouldn't be called when this method was
                            // called by unwrap - unwrap will keep running after
                            // this method returns.
                            unwrap(ctx, channel, ChannelBuffers.EMPTY_BUFFER, 0, 0);
                        }
                        break;
                    case NOT_HANDSHAKING:
                    case NEED_WRAP:
                        break;
                    default:
                        throw new IllegalStateException(
                                "Unexpected handshake status: " + handshakeStatus);
                }

                if (result.bytesProduced() == 0) {
                    break;
                }
            }
        } catch (SSLException e) {
            setHandshakeFailure(channel, e);
            throw e;
        } finally {
            bufferPool.releaseBuffer(outNetBuf);
        }

        if (future == null) {
            future = succeededFuture(channel);
        }

        return future;
    }

    private ChannelBuffer unwrap(
            ChannelHandlerContext ctx, Channel channel,
            ChannelBuffer buffer, int offset, int length) throws SSLException {
        ByteBuffer inNetBuf = buffer.toByteBuffer(offset, length);
        ByteBuffer outAppBuf = bufferPool.acquireBuffer();

        try {
            boolean needsWrap = false;
            loop:
            for (; ; ) {
                SSLEngineResult result;
                boolean needsHandshake = false;
                synchronized (handshakeLock) {
                    if (!handshaken && !handshaking &&
                            !engine.getUseClientMode() &&
                            !engine.isInboundDone() && !engine.isOutboundDone()) {
                        needsHandshake = true;
                    }
                }

                if (needsHandshake) {
                    handshake();
                }

                synchronized (handshakeLock) {
                    result = engine.unwrap(inNetBuf, outAppBuf);

                    switch (result.getStatus()) {
                        case CLOSED:
                            // notify about the CLOSED state of the SSLEngine. See #137
                            sslEngineCloseFuture.setClosed();
                            break;
                        case BUFFER_OVERFLOW:
                            throw new SSLException("SSLEngine.unwrap() reported an impossible buffer overflow.");
                    }

                    final HandshakeStatus handshakeStatus = result.getHandshakeStatus();
                    handleRenegotiation(handshakeStatus);
                    switch (handshakeStatus) {
                        case NEED_UNWRAP:
                            if (inNetBuf.hasRemaining() && !engine.isInboundDone()) {
                                break;
                            } else {
                                break loop;
                            }
                        case NEED_WRAP:
                            wrapNonAppData(ctx, channel);
                            break;
                        case NEED_TASK:
                            runDelegatedTasks();
                            break;
                        case FINISHED:
                            setHandshakeSuccess(channel);
                            needsWrap = true;
                            break loop;
                        case NOT_HANDSHAKING:
                            needsWrap = true;
                            break loop;
                        default:
                            throw new IllegalStateException(
                                    "Unknown handshake status: " + handshakeStatus);
                    }
                }
            }
            if (needsWrap) {
                // wrap() acquires pendingUnencryptedWrites first and then
                // handshakeLock.  If handshakeLock is already hold by the
                // current thread, calling wrap() will lead to a dead lock
                // i.e. pendingUnencryptedWrites -> handshakeLock vs.
                //      handshakeLock -> pendingUnencryptedLock -> handshakeLock
                //
                // There is also the same issue between pendingEncryptedWrites
                // and pendingUnencryptedWrites.
                if (!Thread.holdsLock(handshakeLock) &&
                        !pendingEncryptedWritesLock.isHeldByCurrentThread()) {
                    wrap(ctx, channel);
                }
            }
            outAppBuf.flip();

            if (outAppBuf.hasRemaining()) {
                ChannelBuffer frame = ctx.getChannel().getConfig().getBufferFactory().getBuffer(outAppBuf.remaining());
                // Transfer the bytes to the new ChannelBuffer using some safe method that will also
                // work with "non" heap buffers
                //
                // See https://github.com/netty/netty/issues/329
                frame.writeBytes(outAppBuf);

                return frame;
            } else {
                return null;
            }
        } catch (SSLException e) {
            setHandshakeFailure(channel, e);
            throw e;
        } finally {
            bufferPool.releaseBuffer(outAppBuf);
        }
    }

    private void handleRenegotiation(HandshakeStatus handshakeStatus) {
        synchronized (handshakeLock) {
            if (handshakeStatus == HandshakeStatus.NOT_HANDSHAKING ||
                    handshakeStatus == HandshakeStatus.FINISHED) {
                // Not handshaking
                return;
            }

            if (!handshaken) {
                // Not renegotiation
                return;
            }

            final boolean renegotiate;
            if (handshaking) {
                // Renegotiation in progress or failed already.
                // i.e. Renegotiation check has been done already below.
                return;
            }

            if (engine.isInboundDone() || engine.isOutboundDone()) {
                // Not handshaking but closing.
                return;
            }

            if (isEnableRenegotiation()) {
                // Continue renegotiation.
                renegotiate = true;
            } else {
                // Do not renegotiate.
                renegotiate = false;
                // Prevent reentrance of this method.
                handshaking = true;
            }

            if (renegotiate) {
                // Renegotiate.
                handshake();
            } else {
                // Raise an exception.
                fireExceptionCaught(
                        ctx, new SSLException(
                                "renegotiation attempted by peer; " +
                                        "closing the connection"));

                // Close the connection to stop renegotiation.
                Channels.close(ctx, succeededFuture(ctx.getChannel()));
            }
        }
    }

    private void runDelegatedTasks() {
        for (; ; ) {
            final Runnable task;
            synchronized (handshakeLock) {
                task = engine.getDelegatedTask();
            }

            if (task == null) {
                break;
            }

            delegatedTaskExecutor.execute(new Runnable() {
                public void run() {
                    synchronized (handshakeLock) {
                        task.run();
                    }
                }
            });
        }
    }

    private void setHandshakeSuccess(Channel channel) {
        synchronized (handshakeLock) {
            handshaking = false;
            handshaken = true;

            if (handshakeFuture == null) {
                handshakeFuture = future(channel);
            }
            cancelHandshakeTimeout();
        }

        handshakeFuture.setSuccess();
    }

    private void setHandshakeFailure(Channel channel, SSLException cause) {
        synchronized (handshakeLock) {
            if (!handshaking) {
                return;
            }
            handshaking = false;
            handshaken = false;

            if (handshakeFuture == null) {
                handshakeFuture = future(channel);
            }

            // cancel the timeout now
            cancelHandshakeTimeout();

            // Release all resources such as internal buffers that SSLEngine
            // is managing.

            engine.closeOutbound();

            try {
                engine.closeInbound();
            } catch (SSLException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "SSLEngine.closeInbound() raised an exception after " +
                                    "a handshake failure.", e);
                }
            }
        }

        handshakeFuture.setFailure(cause);
        if (closeOnSSLException) {
            Channels.close(ctx, future(channel));
        }
    }

    private void closeOutboundAndChannel(
            final ChannelHandlerContext context, final ChannelStateEvent e) {
        if (!e.getChannel().isConnected()) {
            context.sendDownstream(e);
            return;
        }

        boolean passthrough = true;
        try {
            try {
                unwrap(context, e.getChannel(), ChannelBuffers.EMPTY_BUFFER, 0, 0);
            } catch (SSLException ex) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Failed to unwrap before sending a close_notify message", ex);
                }
            }

            if (!engine.isOutboundDone()) {
                if (sentCloseNotify.compareAndSet(false, true)) {
                    engine.closeOutbound();
                    try {
                        Channels.close(context, e.getFuture());
                        //ChannelFuture closeNotifyFuture = wrapNonAppData(context, e.getChannel());
                        //closeNotifyFuture.addListener(
                        //        new ClosingChannelFutureListener(context, e));
                        //closeNotifyFuture.addListener(ChannelFutureListener.CLOSE);
                        passthrough = true;
                        //} catch (SSLException ex) {
                    } catch (Exception ex) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Failed to encode a close_notify message", ex);
                        }
                    }
                }
            }
        } finally {
            if (passthrough) {
                context.sendDownstream(e);
            }
        }
    }

    private static final class PendingWrite {
        final ChannelFuture future;
        final ByteBuffer outAppBuf;

        PendingWrite(ChannelFuture future, ByteBuffer outAppBuf) {
            this.future = future;
            this.outAppBuf = outAppBuf;
        }
    }

    private static final class ClosingChannelFutureListener implements ChannelFutureListener {

        private final ChannelHandlerContext context;
        private final ChannelStateEvent e;

        ClosingChannelFutureListener(
                ChannelHandlerContext context, ChannelStateEvent e) {
            this.context = context;
            this.e = e;
        }

        public void operationComplete(ChannelFuture closeNotifyFuture) throws Exception {
            if (!(closeNotifyFuture.getCause() instanceof ClosedChannelException)) {
                Channels.close(context, e.getFuture());
            } else {
                e.getFuture().setSuccess();
            }
        }
    }

    @Override
    public void beforeAdd(ChannelHandlerContext ctx) throws Exception {
        super.beforeAdd(ctx);
        this.ctx = ctx;
    }

    /**
     * Fail all pending writes which we were not able to flush out
     */
    @Override
    public void afterRemove(ChannelHandlerContext ctx) throws Exception {

        // there is no need for synchronization here as we do not receive downstream events anymore
        Throwable cause = null;
        for (; ; ) {
            PendingWrite pw = pendingUnencryptedWrites.poll();
            if (pw == null) {
                break;
            }
            if (cause == null) {
                cause = new IOException("Unable to write data");
            }
            pw.future.setFailure(cause);
        }

        for (; ; ) {
            MessageEvent ev = pendingEncryptedWrites.poll();
            if (ev == null) {
                break;
            }
            if (cause == null) {
                cause = new IOException("Unable to write data");
            }
            ev.getFuture().setFailure(cause);
        }

        if (cause != null) {
            fireExceptionCaughtLater(ctx, cause);
        }
    }

    /**
     * Calls {@link #handshake()} once the {@link org.elasticsearch.common.netty.channel.Channel} is connected
     */
    @Override
    public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
        if (issueHandshake) {
            // issue and handshake and add a listener to it which will fire an exception event if
            // an exception was thrown while doing the handshake
            handshake().addListener(new ChannelFutureListener() {

                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        // Send the event upstream after the handshake was completed without an error.
                        //
                        // See https://github.com/netty/netty/issues/358
                        ctx.sendUpstream(e);
                    }
                }
            });
        } else {
            super.channelConnected(ctx, e);
        }
    }

    /**
     * Loop over all the pending writes and fail them.
     * <p/>
     * See <a href="https://github.com/netty/netty/issues/305">#305</a> for more details.
     */
    @Override
    public void channelClosed(final ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // Move the fail of the writes to the IO-Thread to prevent possible deadlock
        // See https://github.com/netty/netty/issues/989
        ctx.getPipeline().execute(new Runnable() {
            public void run() {
                if (!pendingUnencryptedWritesLock.tryLock()) {
                    return;
                }

                Throwable cause = null;
                try {
                    for (; ; ) {
                        PendingWrite pw = pendingUnencryptedWrites.poll();
                        if (pw == null) {
                            break;
                        }
                        if (cause == null) {
                            cause = new ClosedChannelException();
                        }
                        pw.future.setFailure(cause);
                    }

                    for (; ; ) {
                        MessageEvent ev = pendingEncryptedWrites.poll();
                        if (ev == null) {
                            break;
                        }
                        if (cause == null) {
                            cause = new ClosedChannelException();
                        }
                        ev.getFuture().setFailure(cause);
                    }
                } finally {
                    pendingUnencryptedWritesLock.unlock();
                }

                if (cause != null) {
                    fireExceptionCaught(ctx, cause);
                }
            }
        });

        super.channelClosed(ctx, e);
    }

    private final class SSLEngineInboundCloseFuture extends DefaultChannelFuture {
        public SSLEngineInboundCloseFuture() {
            super(null, true);
        }

        void setClosed() {
            super.setSuccess();
        }

        @Override
        public Channel getChannel() {
            if (ctx == null) {
                // Maybe we should better throw an IllegalStateException() ?
                return null;
            } else {
                return ctx.getChannel();
            }
        }

        @Override
        public boolean setSuccess() {
            return false;
        }

        @Override
        public boolean setFailure(Throwable cause) {
            return false;
        }
    }
}