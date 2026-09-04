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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.transaction.PreparedTransactionHandle;
import org.apache.rocketmq.common.transaction.RecoverableTransactionCheckpoint;
import org.apache.rocketmq.common.transaction.RecoverableTransactionOwner;
import org.apache.rocketmq.common.transaction.RecoverableTransactionOwnerClaim;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.ClaimMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.RecoverableTransactionRequestBody;
import org.apache.rocketmq.remoting.protocol.body.RecoverableTransactionResponseBody;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecoverableTransactionProcessorTest {
    private static final String TOPIC = "recoverable-processor-test";
    private static final String GROUP = "recoverable-processor-group";
    private static final String PREFIX = "recoverable-processor-prefix";

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private BrokerController controller;

    @After
    public void tearDown() {
        if (controller != null) {
            controller.shutdown();
            controller.getMessageStore().destroy();
        }
    }

    @Test
    public void rejectsForgedPayloadHandleAndProtectedTopic() throws Exception {
        controller = startBroker(temporaryFolder.newFolder("validation"));
        RecoverableTransactionOwner owner = freshOwner();
        RecoverableTransactionProcessor processor = new RecoverableTransactionProcessor(controller);
        byte[] payload = "value".getBytes(StandardCharsets.UTF_8);

        RecoverableTransactionRequestBody forgedDigest = prepareBody(owner, 30, 0, TOPIC, payload);
        forgedDigest.setPayloadDigest("forged-digest");
        forgedDigest.setHandleId(RecoverableTransactionCheckpoint.computeHandleId(PREFIX,
            owner.getOwnerEpoch(), owner.getClaimantEpoch(), owner.getOwnerSlot(), 30, 0,
            "forged-digest"));
        assertThat(process(processor, forgedDigest).getCode())
            .isEqualTo(ResponseCode.RECOVERABLE_TRANSACTION_INVALID_REQUEST);

        RecoverableTransactionRequestBody forgedHandle = prepareBody(owner, 30, 0, TOPIC, payload);
        forgedHandle.setHandleId("forged-handle");
        assertThat(process(processor, forgedHandle).getCode())
            .isEqualTo(ResponseCode.RECOVERABLE_TRANSACTION_INVALID_REQUEST);

        RecoverableTransactionRequestBody protectedTopic = prepareBody(owner, 30, 0,
            TopicValidator.RMQ_SYS_RECOVERABLE_TRANS_OUTCOME_TOPIC, payload);
        assertThat(process(processor, protectedTopic).getCode())
            .isEqualTo(ResponseCode.RECOVERABLE_TRANSACTION_INVALID_REQUEST);
    }

    @Test
    public void concurrentDuplicatePrepareReturnsOneCanonicalHandle() throws Exception {
        controller = startBroker(temporaryFolder.newFolder("duplicate"));
        RecoverableTransactionOwner owner = freshOwner();
        RecoverableTransactionProcessor processor = new RecoverableTransactionProcessor(controller);
        RecoverableTransactionRequestBody body = prepareBody(owner, 31, 0, TOPIC,
            "duplicate".getBytes(StandardCharsets.UTF_8));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<RemotingCommand> left = executor.submit(() -> {
                start.await();
                return process(processor, body);
            });
            Future<RemotingCommand> right = executor.submit(() -> {
                start.await();
                return process(processor, body);
            });
            start.countDown();
            RemotingCommand leftResponse = left.get(10, TimeUnit.SECONDS);
            RemotingCommand rightResponse = right.get(10, TimeUnit.SECONDS);
            assertThat(leftResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
            assertThat(rightResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
            PreparedTransactionHandle leftHandle = responseHandle(leftResponse);
            PreparedTransactionHandle rightHandle = responseHandle(rightResponse);
            assertThat(rightHandle).isEqualTo(leftHandle);
            RecoverableTransactionCheckpoint checkpoint = new RecoverableTransactionCheckpoint(
                Arrays.asList(leftHandle));
            assertThat(controller.getRecoverableTransactionOutcomeStore().query(checkpoint).getPreparedCount())
                .isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    private RecoverableTransactionOwner freshOwner() throws Exception {
        return controller.getRecoverableTransactionOutcomeStore().claim(GROUP,
            new RecoverableTransactionOwnerClaim(PREFIX, "job", 0, 1, -1, -1, ClaimMode.FRESH));
    }

    private RecoverableTransactionRequestBody prepareBody(RecoverableTransactionOwner owner, long checkpointId,
        long sequence, String topic, byte[] payload) {
        String payloadDigest = RecoverableTransactionCheckpoint.sha256(payload);
        RecoverableTransactionRequestBody body = new RecoverableTransactionRequestBody();
        body.setProtocolVersion(RecoverableTransactionProtocol.CURRENT_VERSION);
        body.setOperation(RecoverableTransactionProtocol.Operation.PREPARE.name());
        body.setProducerGroup(owner.getProducerGroup());
        body.setTransactionalIdPrefix(owner.getTransactionalIdPrefix());
        body.setOwnerSlot(owner.getOwnerSlot());
        body.setOwnerEpoch(owner.getOwnerEpoch());
        body.setClaimantEpoch(owner.getClaimantEpoch());
        body.setClaimantIdentity(owner.getClaimantIdentity());
        body.setRestoredCheckpointId(owner.getRestoredCheckpointId());
        body.setCheckpointId(checkpointId);
        body.setMessageSequence(sequence);
        body.setPayloadDigest(payloadDigest);
        body.setHandleId(RecoverableTransactionCheckpoint.computeHandleId(PREFIX, owner.getOwnerEpoch(),
            owner.getClaimantEpoch(), owner.getOwnerSlot(), checkpointId, sequence, payloadDigest));
        body.setTopic(topic);
        body.setQueueId(0);
        body.setBornTimestamp(System.currentTimeMillis());
        body.setMessageBody(payload);
        return body;
    }

    private RemotingCommand process(RecoverableTransactionProcessor processor,
        RecoverableTransactionRequestBody body) throws Exception {
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 10911));
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        when(context.channel()).thenReturn(channel);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RECOVERABLE_TRANSACTION, null);
        request.setBody(body.encode());
        return processor.processRequest(context, request);
    }

    private PreparedTransactionHandle responseHandle(RemotingCommand response) {
        RecoverableTransactionResponseBody body = RecoverableTransactionResponseBody.decode(
            response.getBody(), RecoverableTransactionResponseBody.class);
        assertThat(body.getHandles()).hasSize(1);
        return body.getHandles().get(0).toHandle();
    }

    private BrokerController startBroker(File root) throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName("recoverable-processor-broker");
        brokerConfig.setBrokerIP1("127.0.0.1");
        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(0);
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(root.getAbsolutePath());
        storeConfig.setStorePathCommitLog(new File(root, "commitlog").getAbsolutePath());
        storeConfig.setMappedFileSizeCommitLog(1024 * 1024);
        storeConfig.setHaListenPort(0);
        BrokerController broker = new BrokerController(brokerConfig, serverConfig,
            new NettyClientConfig(), storeConfig);
        assertThat(broker.initialize()).isTrue();
        broker.setTransactionalMessageService(new TransactionalMessageServiceImpl(
            new TransactionalMessageBridge(broker, broker.getMessageStore())));
        broker.getTopicConfigManager().updateTopicConfig(new TopicConfig(TOPIC, 1, 1));
        broker.start();
        return broker;
    }
}
