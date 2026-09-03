/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.local.LocalAddress;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NettyRemotingAbstractTest {
    @Spy
    private NettyRemotingAbstract remotingAbstract = new NettyRemotingClient(new NettyClientConfig());

    @Test
    public void testProcessResponseCommand() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(0);
        ResponseFuture responseFuture = new ResponseFuture(null, 1, 3000, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {
                assertThat(semaphore.availablePermits()).isEqualTo(0);
            }

            @Override
            public void operationFail(Throwable throwable) {

            }
        }, new SemaphoreReleaseOnlyOnce(semaphore));

        remotingAbstract.responseTable.putIfAbsent(1, responseFuture);

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "Foo");
        response.setOpaque(1);
        remotingAbstract.processResponseCommand(null, response);

        // Acquire the release permit after call back
        semaphore.acquire(1);
        assertThat(semaphore.availablePermits()).isEqualTo(0);
    }

    @Test
    public void testProcessResponseCommand_NullCallBack() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(0);
        ResponseFuture responseFuture = new ResponseFuture(null, 1, 3000, null,
            new SemaphoreReleaseOnlyOnce(semaphore));

        remotingAbstract.responseTable.putIfAbsent(1, responseFuture);

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "Foo");
        response.setOpaque(1);
        remotingAbstract.processResponseCommand(null, response);

        assertThat(semaphore.availablePermits()).isEqualTo(1);
    }

    @Test
    public void testProcessResponseCommand_RunCallBackInCurrentThread() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(0);
        ResponseFuture responseFuture = new ResponseFuture(null, 1, 3000, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {
                assertThat(semaphore.availablePermits()).isEqualTo(0);
            }

            @Override
            public void operationFail(Throwable throwable) {

            }
        }, new SemaphoreReleaseOnlyOnce(semaphore));

        remotingAbstract.responseTable.putIfAbsent(1, responseFuture);
        when(remotingAbstract.getCallbackExecutor()).thenReturn(null);

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "Foo");
        response.setOpaque(1);
        remotingAbstract.processResponseCommand(null, response);

        // Acquire the release permit after call back finished in current thread
        semaphore.acquire(1);
        assertThat(semaphore.availablePermits()).isEqualTo(0);
    }

    @Test
    public void testScanResponseTable() {
        int dummyId = 1;
        // mock timeout
        ResponseFuture responseFuture = new ResponseFuture(null, dummyId, -1000, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {

            }

            @Override
            public void operationFail(Throwable throwable) {

            }
        }, null);
        remotingAbstract.responseTable.putIfAbsent(dummyId, responseFuture);
        remotingAbstract.scanResponseTable();
        assertNull(remotingAbstract.responseTable.get(dummyId));
    }

    @Test
    public void testProcessRequestCommand() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(0);
        RemotingCommand request = RemotingCommand.createRequestCommand(1, null);
        ResponseFuture responseFuture = new ResponseFuture(null, 1, request, 3000,
            new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {

                }

                @Override
                public void operationSucceed(RemotingCommand response) {
                    assertThat(semaphore.availablePermits()).isEqualTo(0);
                }

                @Override
                public void operationFail(Throwable throwable) {

                }
            }, new SemaphoreReleaseOnlyOnce(semaphore));

        remotingAbstract.responseTable.putIfAbsent(1, responseFuture);
        RemotingCommand response = RemotingCommand.createResponseCommand(0, "Foo");
        response.setOpaque(1);
        remotingAbstract.processResponseCommand(null, response);

        // Acquire the release permit after call back
        semaphore.acquire(1);
        assertThat(semaphore.availablePermits()).isEqualTo(0);
    }

    /**
     * When {@code writeAndFlush} fails, the netty-side {@code f.cause()} must reach the caller.
     *
     * <p>Regression for a real incident: direct memory was exhausted, so netty could not allocate
     * an arena chunk and threw {@code OutOfMemoryError}. Because {@code requestFail} never
     * recorded a cause, the caller only saw a {@code RemotingSendRequestException} with a null
     * cause, and the accompanying {@code log.warn} did not pass the throwable either. The only
     * information that explained the failure was discarded, so the outage looked like "send
     * failures plus a reconnect storm" and pointed nowhere near memory.
     */
    @Test
    public void testWriteFailurePropagatesNettyCauseToCaller() throws InterruptedException {
        final Throwable writeFailure =
                new OutOfMemoryError("Cannot reserve 16777216 bytes of direct buffer memory");
        Channel channel = new MockChannel() {
            @Override
            public ChannelFuture writeAndFlush(Object msg) {
                // An already-failed promise on the immediate executor: addListener notifies
                // synchronously, exercising the same code path as production.
                DefaultChannelPromise promise =
                        new DefaultChannelPromise(this, ImmediateEventExecutor.INSTANCE);
                promise.setFailure(writeFailure);
                return promise;
            }

            @Override
            public LocalAddress remoteAddress() {
                return new LocalAddress("write-failure-test");
            }
        };

        final Semaphore semaphore = new Semaphore(0);
        final AtomicReference<Throwable> observed = new AtomicReference<>();
        remotingAbstract.invokeAsyncImpl(channel, RemotingCommand.createRequestCommand(1, null), 3000,
                new InvokeCallback() {
                    @Override
                    public void operationComplete(ResponseFuture responseFuture) {

                    }

                    @Override
                    public void operationSucceed(RemotingCommand response) {

                    }

                    @Override
                    public void operationFail(Throwable throwable) {
                        observed.set(throwable);
                        semaphore.release();
                    }
                });

        assertThat(semaphore.tryAcquire(1, 10, TimeUnit.SECONDS)).isTrue();
        assertThat(causeChainOf(observed.get())).contains(writeFailure);
    }

    private static List<Throwable> causeChainOf(Throwable throwable) {
        List<Throwable> chain = new ArrayList<>();
        for (Throwable t = throwable; t != null && !chain.contains(t); t = t.getCause()) {
            chain.add(t);
        }
        return chain;
    }
}