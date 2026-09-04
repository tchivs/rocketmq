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
package org.apache.rocketmq.broker.transaction;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.broker.transaction.RecoverableTransactionOutcomeStore.FailurePoint;
import org.apache.rocketmq.broker.transaction.RecoverableTransactionOutcomeStore.ProtocolException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.transaction.PreparedTransactionHandle;
import org.apache.rocketmq.common.transaction.RecoverableTransactionCheckpoint;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.transaction.RecoverableTransactionOwner;
import org.apache.rocketmq.common.transaction.RecoverableTransactionOwnerClaim;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.ClaimMode;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.Decision;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.ErrorCode;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.Outcome;
import org.apache.rocketmq.common.transaction.RecoverableTransactionResult;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RecoverableTransactionOutcomeStoreTest {
    private static final String TOPIC = "recoverable-transaction-test";
    private static final String GROUP = "recoverable-transaction-group";
    private static final String PREFIX = "recoverable-prefix";

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
    public void duplicateConcurrentCommitAndRollbackAreDeterministic() throws Exception {
        File root = temporaryFolder.newFolder("concurrent");
        controller = startBroker(root);
        RecoverableTransactionOutcomeStore store = controller.getRecoverableTransactionOutcomeStore();
        RecoverableTransactionOwner owner = freshOwner(store);
        long initialVisible = visibleMessages();
        PreparedTransactionHandle first = prepare(store, owner, 1, 0, "first");
        PreparedTransactionHandle second = prepare(store, owner, 1, 1, "second");
        RecoverableTransactionCheckpoint checkpoint = new RecoverableTransactionCheckpoint(
            Arrays.asList(first, second));
        assertThat(visibleMessages()).isEqualTo(initialVisible);
        long before = initialVisible;

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        Future<RecoverableTransactionResult> left = executor.submit(() -> {
            start.await();
            return store.finalizeCheckpoint(owner, checkpoint, Decision.COMMIT);
        });
        Future<RecoverableTransactionResult> right = executor.submit(() -> {
            start.await();
            return store.finalizeCheckpoint(owner, checkpoint, Decision.COMMIT);
        });
        start.countDown();
        assertThat(left.get(10, TimeUnit.SECONDS).getOutcome()).isEqualTo(Outcome.COMMITTED);
        assertThat(right.get(10, TimeUnit.SECONDS).getOutcome()).isEqualTo(Outcome.COMMITTED);
        executor.shutdownNow();
        awaitVisible(before + 2);
        store.finalizeCheckpoint(owner, checkpoint, Decision.COMMIT);
        assertThat(visibleMessages()).isEqualTo(before + 2);

        RecoverableTransactionCheckpoint rollback = new RecoverableTransactionCheckpoint(Arrays.asList(
            prepare(store, owner, 2, 0, "rollback")));
        store.finalizeCheckpoint(owner, rollback, Decision.ROLLBACK);
        assertThat(store.query(rollback).getOutcome()).isEqualTo(Outcome.ROLLED_BACK);
        assertThat(visibleMessages()).isEqualTo(before + 2);
        assertProtocolError(() -> store.finalizeCheckpoint(owner, rollback, Decision.COMMIT),
            ErrorCode.DECISION_CONFLICT);
    }

    @Test
    public void restartAfterFinalAppendAndLostResponseReconstructsCompletion() throws Exception {
        File root = temporaryFolder.newFolder("restart");
        controller = startBroker(root);
        RecoverableTransactionOutcomeStore store = controller.getRecoverableTransactionOutcomeStore();
        RecoverableTransactionOwner owner = freshOwner(store);
        RecoverableTransactionCheckpoint checkpoint = new RecoverableTransactionCheckpoint(Arrays.asList(
            prepare(store, owner, 3, 0, "restart")));
        long before = visibleMessages();
        AtomicBoolean failOnce = new AtomicBoolean(true);
        store.setFailureHook((point, checkpointId, handleId) -> {
            if (point == FailurePoint.FINAL_MESSAGE_APPENDED && failOnce.compareAndSet(true, false)) {
                throw new InjectedFailure();
            }
        });
        assertProtocolError(() -> store.finalizeCheckpoint(owner, checkpoint, Decision.COMMIT),
            ErrorCode.STORAGE_ERROR);
        awaitVisible(before + 1);
        store.setFailureHook((point, checkpointId, handleId) -> {
            if (point == FailurePoint.RESPONSE_WRITTEN) throw new InjectedFailure();
        });
        assertThatThrownBy(() -> store.fireResponseHook(checkpoint.getCheckpointId()))
            .isInstanceOf(InjectedFailure.class);
        store.setFailureHook(null);

        controller.shutdown();
        controller = startBroker(root);
        RecoverableTransactionOutcomeStore recovered = controller.getRecoverableTransactionOutcomeStore();
        assertThat(recovered.query(checkpoint).getOutcome()).isEqualTo(Outcome.COMMITTED);
        assertThat(recovered.finalizeCheckpoint(owner, checkpoint, Decision.COMMIT).getOutcome())
            .isEqualTo(Outcome.COMMITTED);
        awaitVisible(before + 1);
    }

    @Test
    public void resumeCollisionAndTakeoverApplyMonotonicFencing() throws Exception {
        controller = startBroker(temporaryFolder.newFolder("fencing"));
        RecoverableTransactionOutcomeStore store = controller.getRecoverableTransactionOutcomeStore();
        RecoverableTransactionOwner original = freshOwner(store);
        RecoverableTransactionOwner sibling = store.claim(GROUP,
            new RecoverableTransactionOwnerClaim(PREFIX, "job", 1, 1, -1, -1, ClaimMode.FRESH));
        assertThat(sibling.getOwnerEpoch()).isEqualTo(original.getOwnerEpoch());
        assertThat(sibling.getClaimantEpoch()).isGreaterThan(original.getClaimantEpoch());
        assertProtocolError(() -> store.claim("different-group",
            new RecoverableTransactionOwnerClaim(PREFIX, "job", 0, 1, -1, -1, ClaimMode.FRESH)),
            ErrorCode.OWNERSHIP_CONFLICT);
        RecoverableTransactionCheckpoint adopted = new RecoverableTransactionCheckpoint(Arrays.asList(
            prepare(store, original, 5, 0, "adopted")));
        RecoverableTransactionCheckpoint later = new RecoverableTransactionCheckpoint(Arrays.asList(
            prepare(store, original, 6, 0, "later")));

        RecoverableTransactionOwner resumed = store.claim(GROUP,
            new RecoverableTransactionOwnerClaim(PREFIX, "job", 0, 2, 5,
                original.getOwnerEpoch(), ClaimMode.RESUME));
        assertThat(resumed.getClaimantEpoch()).isGreaterThan(original.getClaimantEpoch());
        RecoverableTransactionOwner forgedRestore = new RecoverableTransactionOwner(
            resumed.getProtocolVersion(), resumed.getClusterName(), resumed.getBrokerName(),
            resumed.getBrokerAddress(), resumed.getProducerGroup(), resumed.getTransactionalIdPrefix(),
            resumed.getClaimantIdentity(), resumed.getOwnerSlot(), resumed.getOwnerEpoch(),
            resumed.getClaimantEpoch(), resumed.getRestoredCheckpointId() + 1);
        assertProtocolError(() -> store.finalizeCheckpoint(forgedRestore, adopted, Decision.COMMIT),
            ErrorCode.STALE_CLAIMANT_EPOCH);
        assertThat(store.query(later).getOutcome()).isEqualTo(Outcome.ROLLED_BACK);
        assertThat(store.finalizeCheckpoint(original, adopted, Decision.COMMIT).getOutcome())
            .isEqualTo(Outcome.COMMITTED);
        assertProtocolError(() -> store.validatePrepare(original, "stale", "digest", 7, 0, TOPIC, 0),
            ErrorCode.STALE_CLAIMANT_EPOCH);
        assertProtocolError(() -> store.claim(GROUP,
            new RecoverableTransactionOwnerClaim(PREFIX, "other", 0, 0, -1, -1, ClaimMode.FRESH)),
            ErrorCode.OWNERSHIP_CONFLICT);

        RecoverableTransactionCheckpoint takeoverRollback = new RecoverableTransactionCheckpoint(Arrays.asList(
            prepare(store, resumed, 7, 0, "takeover")));
        RecoverableTransactionOwner takeover = store.claim(GROUP,
            new RecoverableTransactionOwnerClaim(PREFIX, "replacement", 0, 0, -1,
                resumed.getOwnerEpoch(), ClaimMode.TAKEOVER));
        assertThat(takeover.getOwnerEpoch()).isGreaterThan(resumed.getOwnerEpoch());
        assertThat(store.query(takeoverRollback).getOutcome()).isEqualTo(Outcome.ROLLED_BACK);
        assertThat(store.query(adopted).getOutcome()).isEqualTo(Outcome.COMMITTED);
        assertProtocolError(() -> store.validatePrepare(resumed, "fenced", "digest", 8, 0, TOPIC, 0),
            ErrorCode.STALE_OWNER_EPOCH);
    }

    @Test
    public void rollbackPersistsDecisionAndPerHandleTombstone() throws Exception {
        controller = startBroker(temporaryFolder.newFolder("rollback-tombstone"));
        RecoverableTransactionOutcomeStore store = controller.getRecoverableTransactionOutcomeStore();
        RecoverableTransactionOwner owner = freshOwner(store);
        awaitQueueOffset(TopicValidator.RMQ_SYS_RECOVERABLE_TRANS_OUTCOME_TOPIC, 1);
        long before = queueOffset(TopicValidator.RMQ_SYS_RECOVERABLE_TRANS_OUTCOME_TOPIC);
        RecoverableTransactionCheckpoint checkpoint = new RecoverableTransactionCheckpoint(Arrays.asList(
            prepare(store, owner, 20, 0, "rollback-tombstone")));

        assertThat(store.finalizeCheckpoint(owner, checkpoint, Decision.ROLLBACK).getOutcome())
            .isEqualTo(Outcome.ROLLED_BACK);
        awaitQueueOffset(TopicValidator.RMQ_SYS_RECOVERABLE_TRANS_OUTCOME_TOPIC, before + 2);
        assertThat(queueOffset(TopicValidator.RMQ_SYS_RECOVERABLE_TRANS_OUTCOME_TOPIC))
            .isEqualTo(before + 2);
    }

    @Test
    public void completionDispatchedBeforePreparedHandleIsNotLost() throws Exception {
        controller = startBroker(temporaryFolder.newFolder("out-of-order"));
        RecoverableTransactionOutcomeStore store = controller.getRecoverableTransactionOutcomeStore();
        RecoverableTransactionOwner owner = freshOwner(store);
        PreparedTransactionHandle handle = syntheticHandle(owner, 21, 0, 900);
        RecoverableTransactionCheckpoint checkpoint = new RecoverableTransactionCheckpoint(Arrays.asList(handle));

        Map<String, String> completion = baseRecordProperties();
        completion.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE, "COMMITTED");
        completion.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_HANDLE, handle.getHandleId());
        store.dispatch(dispatch(TOPIC, 901, completion));
        store.recordPrepared(owner, handle);
        store.dispatch(dispatch(TopicValidator.RMQ_SYS_RECOVERABLE_TRANS_OUTCOME_TOPIC, 902,
            decisionProperties(checkpoint, Decision.COMMIT)));

        assertThat(store.query(checkpoint).getOutcome()).isEqualTo(Outcome.COMMITTED);
    }

    @Test
    public void firstReplayDecisionWinsAndConflictFailsStrictRecovery() throws Exception {
        controller = startBroker(temporaryFolder.newFolder("decision-conflict"));
        RecoverableTransactionOutcomeStore store = controller.getRecoverableTransactionOutcomeStore();
        RecoverableTransactionOwner owner = freshOwner(store);
        RecoverableTransactionCheckpoint checkpoint = new RecoverableTransactionCheckpoint(Arrays.asList(
            prepare(store, owner, 22, 0, "immutable")));
        Map<String, String> commit = decisionProperties(checkpoint, Decision.COMMIT);
        Map<String, String> rollback = decisionProperties(checkpoint, Decision.ROLLBACK);

        appendMarkedRecord(commit);
        appendMarkedRecord(rollback);
        store.dispatch(dispatch(TopicValidator.RMQ_SYS_RECOVERABLE_TRANS_OUTCOME_TOPIC, 1000, commit));
        store.dispatch(dispatch(TopicValidator.RMQ_SYS_RECOVERABLE_TRANS_OUTCOME_TOPIC, 1001, rollback));
        assertThat(store.query(checkpoint).getDecision()).isEqualTo(Decision.COMMIT);
        assertThatThrownBy(store::recover).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void malformedMarkedRecordFailsStrictRecovery() throws Exception {
        controller = startBroker(temporaryFolder.newFolder("malformed-replay"));
        RecoverableTransactionOutcomeStore store = controller.getRecoverableTransactionOutcomeStore();
        freshOwner(store);
        Map<String, String> malformed = baseRecordProperties();
        malformed.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE, "UNKNOWN");
        appendMarkedRecord(malformed);

        assertThatThrownBy(store::recover).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void unmarkedHalfMessagesRemainOnTheClassicCheckPath() throws Exception {
        controller = startBroker(temporaryFolder.newFolder("classic"));
        MessageExtBrokerInner classic = new MessageExtBrokerInner();
        classic.setTopic(TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC);
        assertThat(controller.getRecoverableTransactionOutcomeStore().resolveCheck(classic))
            .isEqualTo(RecoverableTransactionOutcomeStore.CheckResolution.CLASSIC);
    }

    private RecoverableTransactionOwner freshOwner(RecoverableTransactionOutcomeStore store)
        throws ProtocolException {
        return store.claim(GROUP, new RecoverableTransactionOwnerClaim(PREFIX, "job", 0, 1,
            -1, -1, ClaimMode.FRESH));
    }

    private PreparedTransactionHandle prepare(RecoverableTransactionOutcomeStore store,
        RecoverableTransactionOwner owner, long checkpoint, long sequence, String value) throws Exception {
        byte[] body = value.getBytes(StandardCharsets.UTF_8);
        String payloadDigest = RecoverableTransactionCheckpoint.sha256(body);
        String handleId = RecoverableTransactionCheckpoint.computeHandleId(PREFIX, owner.getOwnerEpoch(),
            owner.getClaimantEpoch(), owner.getOwnerSlot(), checkpoint, sequence, payloadDigest);
        store.validatePrepare(owner, handleId, payloadDigest, checkpoint, sequence, TOPIC, 0);
        MessageExtBrokerInner message = new MessageExtBrokerInner();
        message.setTopic(TOPIC);
        message.setQueueId(0);
        message.setBody(body);
        message.setBornTimestamp(System.currentTimeMillis());
        message.setBornHost(controller.getStoreHost());
        message.setStoreHost(controller.getStoreHost());
        message.setWaitStoreMsgOK(true);
        message.setSysFlag(MessageSysFlag.TRANSACTION_NOT_TYPE);
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_PRODUCER_GROUP, GROUP);
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION, "true");
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_VERSION,
            Integer.toString(RecoverableTransactionProtocol.CURRENT_VERSION));
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PREFIX, PREFIX);
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_EPOCH,
            Long.toString(owner.getOwnerEpoch()));
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_EPOCH,
            Long.toString(owner.getClaimantEpoch()));
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_SLOT, "0");
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CHECKPOINT,
            Long.toString(checkpoint));
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_SEQUENCE,
            Long.toString(sequence));
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_HANDLE, handleId);
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PAYLOAD_DIGEST,
            payloadDigest);
        MessageAccessor.putProperty(message, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE,
            "PREPARED");
        MessageClientIDSetter.setUniqID(message);
        message.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));
        PutMessageResult put = controller.getTransactionalMessageService().prepareMessage(message);
        assertThat(put).isNotNull();
        assertThat(put.isOk()).as("half-message prepare status: %s", put).isTrue();
        assertThat(put.getAppendMessageResult()).isNotNull();
        PreparedTransactionHandle handle = new PreparedTransactionHandle(
            RecoverableTransactionProtocol.CURRENT_VERSION, owner.getClusterName(), owner.getBrokerName(),
            owner.getBrokerAddress(), TOPIC, 0, GROUP, PREFIX, owner.getOwnerEpoch(), owner.getClaimantEpoch(),
            0, checkpoint, sequence, handleId, payloadDigest, put.getAppendMessageResult().getMsgId(),
            MessageClientIDSetter.getUniqID(message), 0, put.getAppendMessageResult().getLogicsOffset(),
            put.getAppendMessageResult().getWroteOffset());
        store.recordPrepared(owner, handle);
        return handle;
    }

    private PreparedTransactionHandle syntheticHandle(RecoverableTransactionOwner owner, long checkpoint,
        long sequence, long offset) {
        String payloadDigest = RecoverableTransactionCheckpoint.sha256(
            ("synthetic-" + sequence).getBytes(StandardCharsets.UTF_8));
        String handleId = RecoverableTransactionCheckpoint.computeHandleId(PREFIX, owner.getOwnerEpoch(),
            owner.getClaimantEpoch(), owner.getOwnerSlot(), checkpoint, sequence, payloadDigest);
        return new PreparedTransactionHandle(RecoverableTransactionProtocol.CURRENT_VERSION,
            owner.getClusterName(), owner.getBrokerName(), owner.getBrokerAddress(), TOPIC, 0, GROUP, PREFIX,
            owner.getOwnerEpoch(), owner.getClaimantEpoch(), owner.getOwnerSlot(), checkpoint, sequence,
            handleId, payloadDigest, "message-" + offset, "transaction-" + offset, 0, offset, offset);
    }

    private Map<String, String> baseRecordProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION, "true");
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_VERSION,
            Integer.toString(RecoverableTransactionProtocol.CURRENT_VERSION));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PREFIX, PREFIX);
        return properties;
    }

    private Map<String, String> decisionProperties(RecoverableTransactionCheckpoint checkpoint,
        Decision decision) {
        Map<String, String> properties = baseRecordProperties();
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE, "DECISION");
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_EPOCH,
            Long.toString(checkpoint.getOwnerEpoch()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_EPOCH,
            Long.toString(checkpoint.getClaimantEpoch()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_SLOT,
            Integer.toString(checkpoint.getOwnerSlot()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CHECKPOINT,
            Long.toString(checkpoint.getCheckpointId()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_DECISION, decision.name());
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_COUNT,
            Integer.toString(checkpoint.getPreparedCount()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_DIGEST, checkpoint.getDigest());
        return properties;
    }

    private DispatchRequest dispatch(String topic, long offset, Map<String, String> properties) {
        return new DispatchRequest(topic, 0, offset, 1, 0, System.currentTimeMillis(), 0,
            null, null, 0, 0, properties);
    }

    private void appendMarkedRecord(Map<String, String> properties) {
        MessageExtBrokerInner record = new MessageExtBrokerInner();
        record.setTopic(TopicValidator.RMQ_SYS_RECOVERABLE_TRANS_OUTCOME_TOPIC);
        record.setQueueId(0);
        record.setBody(new byte[0]);
        record.setBornTimestamp(System.currentTimeMillis());
        record.setBornHost(controller.getStoreHost());
        record.setStoreHost(controller.getStoreHost());
        record.setWaitStoreMsgOK(true);
        record.setSysFlag(MessageSysFlag.TRANSACTION_NOT_TYPE);
        MessageAccessor.setProperties(record, properties);
        MessageClientIDSetter.setUniqID(record);
        record.setPropertiesString(MessageDecoder.messageProperties2String(properties));
        PutMessageResult result = controller.getMessageStore().putMessage(record);
        assertThat(result).isNotNull();
        assertThat(result.isOk()).isTrue();
    }

    private BrokerController startBroker(File root) throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName("recoverable-broker");
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

    private long visibleMessages() {
        return Math.max(controller.getMessageStore().getMaxOffsetInQueue(TOPIC, 0), 0);
    }

    private void awaitVisible(long expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (visibleMessages() != expected && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(visibleMessages()).isEqualTo(expected);
    }

    private long queueOffset(String topic) {
        return Math.max(controller.getMessageStore().getMaxOffsetInQueue(topic, 0), 0);
    }

    private void awaitQueueOffset(String topic, long expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (queueOffset(topic) < expected && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(queueOffset(topic)).isGreaterThanOrEqualTo(expected);
    }

    private void assertProtocolError(ThrowingAction action, ErrorCode errorCode) {
        assertThatThrownBy(action::run).isInstanceOf(ProtocolException.class)
            .extracting(error -> ((ProtocolException) error).getErrorCode()).isEqualTo(errorCode);
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }

    private static final class InjectedFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
