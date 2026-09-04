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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.transaction.PreparedTransactionHandle;
import org.apache.rocketmq.common.transaction.RecoverableTransactionCheckpoint;
import org.apache.rocketmq.common.transaction.RecoverableTransactionOwner;
import org.apache.rocketmq.common.transaction.RecoverableTransactionOwnerClaim;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.ClaimMode;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.Decision;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.ErrorCode;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.Outcome;
import org.apache.rocketmq.common.transaction.RecoverableTransactionResult;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.logfile.MappedFile;

/**
 * Durable broker-side source of truth for recoverable transactions.
 *
 * <p>Owner and decision/tombstone events are ordinary replicated commit-log records in a system topic.
 * Prepared half messages and committed final messages carry the same stable handle id. Commit-log replay invokes
 * {@link #dispatch(DispatchRequest)} before the broker accepts requests, reconstructing every runtime index.</p>
 */
public class RecoverableTransactionOutcomeStore implements CommitLogDispatcher {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private static final byte[] EMPTY_BODY = new byte[0];
    private static final String RECORD_OWNER = "OWNER";
    private static final String RECORD_DECISION = "DECISION";
    private static final String RECORD_ROLLBACK = "ROLLBACK";
    private static final String RECORD_PREPARED = "PREPARED";
    private static final String RECORD_COMMITTED = "COMMITTED";
    private static final int LOCK_COUNT = 256;

    private final BrokerController brokerController;
    private final ConcurrentMap<String, OwnerState> owners = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, HandleState> handles = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CheckpointState> checkpoints = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CompletionEvidence> completionEvidence = new ConcurrentHashMap<>();
    private final ReentrantLock[] locks = new ReentrantLock[LOCK_COUNT];
    private volatile FailureHook failureHook = FailureHook.NOOP;

    public RecoverableTransactionOutcomeStore(BrokerController brokerController) {
        this.brokerController = brokerController;
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    public RecoverableTransactionOwner claim(String producerGroup, RecoverableTransactionOwnerClaim claim)
        throws ProtocolException {
        requireVersion(claim.getProtocolVersion());
        if (producerGroup == null || producerGroup.isEmpty()) {
            throw new ProtocolException(ErrorCode.INVALID_REQUEST, "producer group is required");
        }
        ReentrantLock lock = lockFor(claim.getTransactionalIdPrefix());
        lock.lock();
        try {
            OwnerState current = owners.get(claim.getTransactionalIdPrefix());
            if (current == null) {
                if (claim.getMode() != ClaimMode.FRESH && claim.getMode() != ClaimMode.TAKEOVER) {
                    throw new ProtocolException(ErrorCode.OWNERSHIP_CONFLICT,
                        "cannot resume an owner namespace that has no durable owner record");
                }
                appendOwnerRecord(producerGroup, claim, 1, 1);
                reconcileClaim(claim.getTransactionalIdPrefix(), 1, claim.getOwnerSlot(),
                    claim.getRestoredCheckpointId(), claim.getMode());
                return ownerResult(claim.getTransactionalIdPrefix(), claim.getOwnerSlot());
            }

            ClaimState active = current.claims.get(claim.getOwnerSlot());
            if (active != null && active.identity.equals(claim.claimantIdentity())
                && active.mode == claim.getMode()) {
                if (!producerGroup.equals(current.producerGroup)) {
                    throw new ProtocolException(ErrorCode.OWNERSHIP_CONFLICT,
                        "producer group does not own the transactional id prefix");
                }
                reconcileClaim(claim.getTransactionalIdPrefix(), current.ownerEpoch, claim.getOwnerSlot(),
                    claim.getRestoredCheckpointId(), claim.getMode());
                return ownerResult(claim.getTransactionalIdPrefix(), claim.getOwnerSlot());
            }

            if (claim.getMode() == ClaimMode.FRESH) {
                if (!producerGroup.equals(current.producerGroup)
                    || !claim.getJobId().equals(current.namespaceJobId) || active != null) {
                    throw new ProtocolException(ErrorCode.OWNERSHIP_CONFLICT,
                        "transactional id prefix is already owned");
                }
                appendOwnerRecord(producerGroup, claim, current.ownerEpoch, current.nextClaimantEpoch + 1);
            } else if (claim.getMode() == ClaimMode.RESUME) {
                if (!producerGroup.equals(current.producerGroup)) {
                    throw new ProtocolException(ErrorCode.OWNERSHIP_CONFLICT,
                        "producer group does not own the transactional id prefix");
                }
                if (claim.getExpectedOwnerEpoch() != current.ownerEpoch) {
                    throw new ProtocolException(ErrorCode.STALE_OWNER_EPOCH,
                        "restored owner epoch does not match the durable owner epoch");
                }
                appendOwnerRecord(producerGroup, claim, current.ownerEpoch, current.nextClaimantEpoch + 1);
            } else {
                long nextOwnerEpoch = current.ownerEpoch + 1;
                appendOwnerRecord(producerGroup, claim, nextOwnerEpoch, current.nextClaimantEpoch + 1);
            }
            OwnerState updated = owners.get(claim.getTransactionalIdPrefix());
            reconcileClaim(claim.getTransactionalIdPrefix(), updated.ownerEpoch, claim.getOwnerSlot(),
                claim.getRestoredCheckpointId(), claim.getMode());
            return ownerResult(claim.getTransactionalIdPrefix(), claim.getOwnerSlot());
        } finally {
            lock.unlock();
        }
    }

    public PreparedTransactionHandle validatePrepare(RecoverableTransactionOwner owner, String handleId,
        String payloadDigest, long checkpointId, long messageSequence, String topic, int queueId)
        throws ProtocolException {
        requireVersion(owner.getProtocolVersion());
        ReentrantLock lock = lockFor(owner.getTransactionalIdPrefix());
        lock.lock();
        try {
            validateCurrentOwner(owner);
            HandleState existing = handles.get(handleId);
            if (existing == null) {
                return null;
            }
            PreparedTransactionHandle handle = existing.handle;
            if (!handle.getPayloadDigest().equals(payloadDigest)
                || !handle.getTransactionalIdPrefix().equals(owner.getTransactionalIdPrefix())
                || handle.getOwnerEpoch() != owner.getOwnerEpoch()
                || handle.getOwnerSlot() != owner.getOwnerSlot()
                || handle.getCheckpointId() != checkpointId
                || handle.getMessageSequence() != messageSequence
                || !handle.getTopic().equals(topic) || handle.getQueueId() != queueId) {
                throw new ProtocolException(ErrorCode.HANDLE_COLLISION,
                    "handle id is already associated with different prepared message metadata");
            }
            return handle;
        } finally {
            lock.unlock();
        }
    }

    public PreparedTransactionHandle recordPrepared(RecoverableTransactionOwner owner,
        PreparedTransactionHandle handle) throws ProtocolException {
        ReentrantLock lock = lockFor(handle.getTransactionalIdPrefix());
        lock.lock();
        try {
            validateCurrentOwner(owner);
            if (!owner.getProducerGroup().equals(handle.getProducerGroup())
                || !owner.getTransactionalIdPrefix().equals(handle.getTransactionalIdPrefix())
                || owner.getOwnerEpoch() != handle.getOwnerEpoch()
                || owner.getClaimantEpoch() != handle.getClaimantEpoch()
                || owner.getOwnerSlot() != handle.getOwnerSlot()) {
                throw new ProtocolException(ErrorCode.STALE_CLAIMANT_EPOCH,
                    "prepared handle does not belong to the current claimant");
            }
            return recordPreparedLocked(handle);
        } finally {
            lock.unlock();
        }
    }

    public RecoverableTransactionResult finalizeCheckpoint(RecoverableTransactionOwner owner,
        RecoverableTransactionCheckpoint checkpoint, Decision decision) throws ProtocolException {
        requireVersion(checkpoint.getProtocolVersion());
        ReentrantLock lock = lockFor(checkpoint.getTransactionalIdPrefix());
        lock.lock();
        try {
            validateFinalizeOwner(owner, checkpoint);
            CheckpointState state = requireMatchingCheckpoint(checkpoint);
            boolean idempotent = state.decision != null;
            if (state.decision != null && state.decision != decision) {
                throw new ProtocolException(ErrorCode.DECISION_CONFLICT,
                    "checkpoint already has terminal decision " + state.decision);
            }
            if (state.decision == null) {
                Map<String, String> properties = baseProperties(checkpoint.getTransactionalIdPrefix());
                properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE, RECORD_DECISION);
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
                appendOutcomeRecord(properties);
                failureHook.onFailure(FailurePoint.DECISION_PERSISTED, checkpoint.getCheckpointId(), null);
                state = checkpoints.get(checkpointKey(checkpoint.getTransactionalIdPrefix(),
                    checkpoint.getOwnerEpoch(), checkpoint.getClaimantEpoch(), checkpoint.getOwnerSlot(),
                    checkpoint.getCheckpointId()));
            }
            completeCheckpoint(state);
            return result(state, idempotent);
        } finally {
            lock.unlock();
        }
    }

    public RecoverableTransactionResult abort(RecoverableTransactionCheckpoint checkpoint)
        throws ProtocolException {
        ReentrantLock lock = lockFor(checkpoint.getTransactionalIdPrefix());
        lock.lock();
        try {
            CheckpointState state = requireMatchingCheckpoint(checkpoint);
            if (state.decision == Decision.COMMIT) {
                throw new ProtocolException(ErrorCode.DECISION_CONFLICT,
                    "a committed checkpoint cannot be administratively aborted");
            }
            if (state.decision == null) {
                Map<String, String> properties = baseProperties(checkpoint.getTransactionalIdPrefix());
                properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE, RECORD_DECISION);
                properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_EPOCH,
                    Long.toString(checkpoint.getOwnerEpoch()));
                properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_EPOCH,
                    Long.toString(checkpoint.getClaimantEpoch()));
                properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_SLOT,
                    Integer.toString(checkpoint.getOwnerSlot()));
                properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CHECKPOINT,
                    Long.toString(checkpoint.getCheckpointId()));
                properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_DECISION, Decision.ROLLBACK.name());
                properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_COUNT,
                    Integer.toString(checkpoint.getPreparedCount()));
                properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_DIGEST, checkpoint.getDigest());
                appendOutcomeRecord(properties);
                state = checkpoints.get(checkpointKey(checkpoint.getTransactionalIdPrefix(),
                    checkpoint.getOwnerEpoch(), checkpoint.getClaimantEpoch(), checkpoint.getOwnerSlot(),
                    checkpoint.getCheckpointId()));
            }
            completeCheckpoint(state);
            return result(state, true);
        } finally {
            lock.unlock();
        }
    }

    public RecoverableTransactionResult query(RecoverableTransactionCheckpoint checkpoint)
        throws ProtocolException {
        ReentrantLock lock = lockFor(checkpoint.getTransactionalIdPrefix());
        lock.lock();
        try {
            return result(requireMatchingCheckpoint(checkpoint), true);
        } finally {
            lock.unlock();
        }
    }

    public RecoverableTransactionResult query(String handleId) throws ProtocolException {
        HandleState handle = handles.get(handleId);
        if (handle == null) throw new ProtocolException(ErrorCode.NOT_FOUND, "prepared handle was not found");
        ReentrantLock lock = lockFor(handle.handle.getTransactionalIdPrefix());
        lock.lock();
        try {
            CheckpointState state = checkpoints.get(checkpointKey(handle.handle));
            if (state == null) throw new ProtocolException(ErrorCode.NOT_FOUND, "checkpoint was not found");
            return result(state, true);
        } finally {
            lock.unlock();
        }
    }

    public List<PreparedTransactionHandle> listPending(String transactionalIdPrefix) {
        List<PreparedTransactionHandle> result = new ArrayList<>();
        for (HandleState state : handles.values()) {
            CheckpointState checkpoint = checkpoints.get(checkpointKey(state.handle));
            if (state.handle.getTransactionalIdPrefix().equals(transactionalIdPrefix)
                && state.outcome == Outcome.PENDING && !isFenced(state.handle)
                && (checkpoint == null || checkpoint.decision == null)) {
                result.add(state.handle);
            }
        }
        result.sort(Comparator.comparingLong(PreparedTransactionHandle::getCheckpointId)
            .thenComparingLong(PreparedTransactionHandle::getMessageSequence));
        return result;
    }

    public CheckResolution resolveCheck(MessageExt message) throws ProtocolException {
        String handleId = message.getProperty(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_HANDLE);
        if (handleId == null) return CheckResolution.CLASSIC;
        String prefix = message.getProperty(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PREFIX);
        ReentrantLock lock = lockFor(prefix);
        lock.lock();
        try {
            HandleState handle = handles.get(handleId);
            if (handle == null) {
                recordPreparedLocked(fromHalfMessage(message));
                handle = handles.get(handleId);
            }
            CheckpointState checkpoint = checkpoints.get(checkpointKey(handle.handle));
            if (handle.outcome == Outcome.COMMITTED) {
                brokerController.getTransactionalMessageService().deletePrepareMessage(message);
                return CheckResolution.COMMITTED;
            }
            if (checkpoint != null && checkpoint.decision == Decision.COMMIT) {
                commitHandle(handle);
                brokerController.getTransactionalMessageService().deletePrepareMessage(message);
                return CheckResolution.COMMITTED;
            }
            if (checkpoint != null && checkpoint.decision == Decision.ROLLBACK) {
                rollbackHandle(handle);
                brokerController.getTransactionalMessageService().deletePrepareMessage(message);
                return CheckResolution.ROLLED_BACK;
            }
            if (handle.outcome == Outcome.ROLLED_BACK || isFenced(handle.handle)) {
                if (checkpoint == null) {
                    throw new ProtocolException(ErrorCode.NOT_FOUND, "checkpoint was not found");
                }
                if (checkpoint.decision == null) appendCheckpointDecision(checkpoint, Decision.ROLLBACK);
                rollbackHandle(handle);
                brokerController.getTransactionalMessageService().deletePrepareMessage(message);
                return CheckResolution.ROLLED_BACK;
            }
            return CheckResolution.PENDING;
        } finally {
            lock.unlock();
        }
    }

    /** Rebuilds durable ownership and completion indexes from the full commit log at startup. */
    public void recover() {
        owners.clear();
        handles.clear();
        checkpoints.clear();
        completionEvidence.clear();
        for (MappedFile mappedFile : brokerController.getMessageStore().getCommitLog()
            .getMappedFileQueue().getMappedFiles()) {
            ByteBuffer buffer = mappedFile.sliceByteBuffer();
            while (buffer.hasRemaining()) {
                DispatchRequest request = brokerController.getMessageStore()
                    .checkMessageAndReturnSize(buffer, false, false, true);
                if (!request.isSuccess() || request.getMsgSize() <= 0) break;
                try {
                    processDispatch(request);
                } catch (RuntimeException | ProtocolException e) {
                    throw new IllegalStateException("failed to reconstruct recoverable transaction state at "
                        + request.getCommitLogOffset(), e);
                }
            }
        }
    }

    @Override
    public void dispatch(DispatchRequest request) {
        try {
            processDispatch(request);
        } catch (RuntimeException | ProtocolException e) {
            LOG.error("Failed to dispatch recoverable transaction record, topic={}, commitLogOffset={}",
                request.getTopic(), request.getCommitLogOffset(), e);
        }
    }

    private void processDispatch(DispatchRequest request) throws ProtocolException {
        Map<String, String> properties = request.getPropertiesMap();
        if (properties == null || !"true".equals(properties.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION))) {
            return;
        }
        String prefix = properties.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PREFIX);
        ReentrantLock lock = lockFor(prefix);
        lock.lock();
        try {
            String recordType = properties.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE);
            if (TopicValidator.RMQ_SYS_RECOVERABLE_TRANS_OUTCOME_TOPIC.equals(request.getTopic())) {
                applyOutcomeRecord(properties, request.getCommitLogOffset());
            } else if (RECORD_COMMITTED.equals(recordType)) {
                applyCompletionEvidence(properties.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_HANDLE),
                    Outcome.COMMITTED, request.getCommitLogOffset());
            } else if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(request.getTopic())
                && RECORD_PREPARED.equals(recordType)) {
                recordPreparedLocked(fromDispatchRequest(request));
            } else {
                throw new ProtocolException(ErrorCode.STORAGE_ERROR,
                    "marked recoverable transaction record has an invalid topic or type");
            }
        } finally {
            lock.unlock();
        }
    }

    public void fireResponseHook(long checkpointId) {
        failureHook.onFailure(FailurePoint.RESPONSE_WRITTEN, checkpointId, null);
    }

    public void setFailureHook(FailureHook failureHook) {
        this.failureHook = failureHook == null ? FailureHook.NOOP : failureHook;
    }

    private void completeCheckpoint(CheckpointState checkpoint) throws ProtocolException {
        validateDecisionContents(checkpoint);
        if (checkpoint.decision == null || checkpoint.handles.size() != checkpoint.expectedCount) {
            throw new ProtocolException(ErrorCode.CHECKPOINT_MISMATCH,
                "durable checkpoint decision is missing prepared handles");
        }
        List<HandleState> ordered = new ArrayList<>(checkpoint.handles.values());
        ordered.sort(Comparator.comparingLong(state -> state.handle.getMessageSequence()));
        for (HandleState handle : ordered) {
            if (checkpoint.decision == Decision.COMMIT) {
                if (handle.outcome != Outcome.COMMITTED) commitHandle(handle);
            } else if (handle.outcome != Outcome.ROLLED_BACK) {
                rollbackHandle(handle);
            }
        }
    }

    private void commitHandle(HandleState state) throws ProtocolException {
        if (state.outcome == Outcome.COMMITTED) return;
        if (state.outcome == Outcome.ROLLED_BACK) {
            throw new ProtocolException(ErrorCode.DECISION_CONFLICT, "prepared handle was already rolled back");
        }
        MessageExt half = brokerController.getMessageStore().lookMessageByOffset(
            state.currentHalfCommitLogOffset);
        if (half == null) throw new ProtocolException(ErrorCode.NOT_FOUND, "prepared half message was not found");
        MessageExtBrokerInner message = finalMessage(half, state.handle);
        PutMessageResult putResult = brokerController.getMessageStore().putMessage(message);
        if (putResult == null || !putResult.isOk()) {
            throw new ProtocolException(ErrorCode.STORAGE_ERROR, "final message append failed");
        }
        RuntimeException hookFailure = null;
        try {
            failureHook.onFailure(FailurePoint.FINAL_MESSAGE_APPENDED,
                state.handle.getCheckpointId(), state.handle.getHandleId());
        } catch (RuntimeException e) {
            hookFailure = e;
        }
        applyCompletionEvidence(state.handle.getHandleId(), Outcome.COMMITTED,
            putResult.getAppendMessageResult().getWroteOffset());
        failureHook.onFailure(FailurePoint.RUNTIME_INDEX_UPDATED,
            state.handle.getCheckpointId(), state.handle.getHandleId());
        brokerController.getTransactionalMessageService().deletePrepareMessage(half);
        if (hookFailure != null || putResult.getPutMessageStatus() != PutMessageStatus.PUT_OK) {
            throw new ProtocolException(ErrorCode.STORAGE_ERROR,
                "final message was appended but its acknowledgement was ambiguous", hookFailure);
        }
    }

    private void rollbackHandle(HandleState state) throws ProtocolException {
        if (state.outcome == Outcome.ROLLED_BACK) return;
        if (state.outcome == Outcome.COMMITTED) {
            throw new ProtocolException(ErrorCode.DECISION_CONFLICT, "committed handle cannot be rolled back");
        }
        Map<String, String> properties = handleProperties(state.handle);
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE, RECORD_ROLLBACK);
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_DECISION, Decision.ROLLBACK.name());
        appendOutcomeRecord(properties);
        MessageExt half = brokerController.getMessageStore().lookMessageByOffset(
            state.currentHalfCommitLogOffset);
        if (half != null) brokerController.getTransactionalMessageService().deletePrepareMessage(half);
    }

    private MessageExtBrokerInner finalMessage(MessageExt half, PreparedTransactionHandle handle) {
        MessageExtBrokerInner result = new MessageExtBrokerInner();
        result.setTopic(handle.getTopic());
        result.setQueueId(handle.getQueueId());
        result.setBody(half.getBody());
        result.setFlag(half.getFlag());
        result.setBornTimestamp(half.getBornTimestamp());
        result.setBornHost(half.getBornHost());
        result.setStoreHost(half.getStoreHost());
        result.setReconsumeTimes(half.getReconsumeTimes());
        result.setWaitStoreMsgOK(true);
        result.setTransactionId(handle.getTransactionId());
        result.setSysFlag(MessageSysFlag.resetTransactionValue(half.getSysFlag(),
            MessageSysFlag.TRANSACTION_COMMIT_TYPE));
        result.setQueueOffset(handle.getHalfMessageQueueOffset());
        result.setPreparedTransactionOffset(half.getCommitLogOffset());
        Map<String, String> properties = new HashMap<>(half.getProperties());
        properties.remove(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        properties.remove(MessageConst.PROPERTY_REAL_TOPIC);
        properties.remove(MessageConst.PROPERTY_REAL_QUEUE_ID);
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE, RECORD_COMMITTED);
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_DECISION, Decision.COMMIT.name());
        MessageAccessor.setProperties(result, properties);
        TopicFilterType filterType = (result.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG)
            == MessageSysFlag.MULTI_TAGS_FLAG ? TopicFilterType.MULTI_TAG : TopicFilterType.SINGLE_TAG;
        result.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(filterType, result.getTags()));
        result.setPropertiesString(MessageDecoder.messageProperties2String(properties));
        return result;
    }

    private void appendOwnerRecord(String producerGroup, RecoverableTransactionOwnerClaim claim,
        long ownerEpoch, long claimantEpoch) throws ProtocolException {
        Map<String, String> properties = baseProperties(claim.getTransactionalIdPrefix());
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE, RECORD_OWNER);
        properties.put(MessageConst.PROPERTY_PRODUCER_GROUP, producerGroup);
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_EPOCH, Long.toString(ownerEpoch));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_EPOCH,
            Long.toString(claimantEpoch));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_SLOT,
            Integer.toString(claim.getOwnerSlot()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_JOB_ID, claim.getJobId());
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_ATTEMPT,
            Long.toString(claim.getAttemptNumber()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RESTORED_CHECKPOINT,
            Long.toString(claim.getRestoredCheckpointId()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIM_MODE, claim.getMode().name());
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_ID, claim.claimantIdentity());
        appendOutcomeRecord(properties);
    }

    private void appendOutcomeRecord(Map<String, String> properties) throws ProtocolException {
        MessageExtBrokerInner record = new MessageExtBrokerInner();
        record.setTopic(TopicValidator.RMQ_SYS_RECOVERABLE_TRANS_OUTCOME_TOPIC);
        record.setQueueId(0);
        record.setBody(EMPTY_BODY);
        record.setBornTimestamp(System.currentTimeMillis());
        record.setBornHost(brokerController.getStoreHost());
        record.setStoreHost(brokerController.getStoreHost());
        record.setWaitStoreMsgOK(true);
        record.setSysFlag(MessageSysFlag.TRANSACTION_NOT_TYPE);
        MessageAccessor.setProperties(record, properties);
        MessageClientIDSetter.setUniqID(record);
        record.setPropertiesString(MessageDecoder.messageProperties2String(record.getProperties()));
        PutMessageResult result = brokerController.getMessageStore().putMessage(record);
        if (result == null || !result.isOk()) {
            throw new ProtocolException(ErrorCode.STORAGE_ERROR, "durable outcome record append failed");
        }
        applyOutcomeRecord(properties, result.getAppendMessageResult().getWroteOffset());
        if (result.getPutMessageStatus() != PutMessageStatus.PUT_OK) {
            throw new ProtocolException(ErrorCode.STORAGE_ERROR,
                "durable outcome record append acknowledgement was ambiguous");
        }
    }

    private void applyOutcomeRecord(Map<String, String> properties, long recordOffset)
        throws ProtocolException {
        String type = properties.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE);
        String prefix = properties.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PREFIX);
        if (RECORD_OWNER.equals(type)) {
            long ownerEpoch = longProperty(properties,
                MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_EPOCH);
            long claimantEpoch = longProperty(properties,
                MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_EPOCH);
            int ownerSlot = intProperty(properties, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_SLOT);
            long restoredCheckpoint = longProperty(properties,
                MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RESTORED_CHECKPOINT);
            ClaimMode mode = ClaimMode.valueOf(properties.get(
                MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIM_MODE));
            String producerGroup = properties.get(MessageConst.PROPERTY_PRODUCER_GROUP);
            String jobId = properties.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_JOB_ID);
            String claimantIdentity = properties.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_ID);
            if (ownerEpoch <= 0 || claimantEpoch <= 0 || ownerSlot < 0 || producerGroup == null
                || producerGroup.isEmpty() || jobId == null || jobId.isEmpty() || claimantIdentity == null
                || claimantIdentity.isEmpty()) {
                throw new ProtocolException(ErrorCode.STORAGE_ERROR, "malformed durable owner record");
            }
            OwnerState owner = owners.computeIfAbsent(prefix, ignored -> new OwnerState());
            if (ownerEpoch < owner.ownerEpoch) {
                return;
            }
            if (ownerEpoch > owner.ownerEpoch) {
                owner.ownerEpoch = ownerEpoch;
                owner.producerGroup = producerGroup;
                owner.namespaceJobId = jobId;
                owner.claims.clear();
            } else {
                if (owner.producerGroup != null && !owner.producerGroup.equals(producerGroup)) {
                    throw new ProtocolException(ErrorCode.STORAGE_ERROR,
                        "conflicting producer group in durable owner record");
                }
                if (owner.namespaceJobId != null && mode == ClaimMode.FRESH
                    && !owner.namespaceJobId.equals(jobId)) {
                    throw new ProtocolException(ErrorCode.STORAGE_ERROR,
                        "conflicting job id in durable fresh owner record");
                }
                owner.producerGroup = producerGroup;
                if (owner.namespaceJobId == null) owner.namespaceJobId = jobId;
            }
            ClaimState active = owner.claims.get(ownerSlot);
            if (active != null && claimantEpoch < active.claimantEpoch) {
                return;
            }
            if (active != null && claimantEpoch == active.claimantEpoch
                && (!active.identity.equals(claimantIdentity) || active.mode != mode
                    || active.restoredCheckpointId != restoredCheckpoint)) {
                throw new ProtocolException(ErrorCode.STORAGE_ERROR,
                    "conflicting durable claimant record at the same epoch");
            }
            owner.nextClaimantEpoch = Math.max(owner.nextClaimantEpoch, claimantEpoch);
            owner.claims.put(ownerSlot, new ClaimState(claimantIdentity, claimantEpoch,
                restoredCheckpoint, mode));
        } else if (RECORD_DECISION.equals(type)) {
            long ownerEpoch = longProperty(properties,
                MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_EPOCH);
            long claimantEpoch = longProperty(properties,
                MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_EPOCH);
            int ownerSlot = intProperty(properties, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_SLOT);
            long checkpointId = longProperty(properties,
                MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CHECKPOINT);
            Decision decision = Decision.valueOf(properties.get(
                MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_DECISION));
            int expectedCount = intProperty(properties,
                MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_COUNT);
            String expectedDigest = properties.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_DIGEST);
            if (expectedCount <= 0 || expectedDigest == null || expectedDigest.isEmpty()) {
                throw new ProtocolException(ErrorCode.STORAGE_ERROR, "malformed durable checkpoint decision");
            }
            CheckpointState checkpoint = checkpoints.computeIfAbsent(
                checkpointKey(prefix, ownerEpoch, claimantEpoch, ownerSlot, checkpointId),
                ignored -> new CheckpointState(prefix, ownerEpoch, claimantEpoch, ownerSlot, checkpointId));
            if (checkpoint.decision != null) {
                if (checkpoint.decision != decision || checkpoint.expectedCount != expectedCount
                    || !checkpoint.expectedDigest.equals(expectedDigest)) {
                    throw new ProtocolException(ErrorCode.DECISION_CONFLICT,
                        "durable checkpoint decision is immutable");
                }
                return;
            }
            checkpoint.decision = decision;
            checkpoint.expectedCount = expectedCount;
            checkpoint.expectedDigest = expectedDigest;
            validateDecisionContents(checkpoint);
        } else if (RECORD_ROLLBACK.equals(type)) {
            applyCompletionEvidence(properties.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_HANDLE),
                Outcome.ROLLED_BACK, recordOffset);
        } else {
            throw new ProtocolException(ErrorCode.STORAGE_ERROR, "unknown durable recoverable record type");
        }
    }

    private void reconcileClaim(String prefix, long ownerEpoch, int ownerSlot, long restoredCheckpoint,
        ClaimMode mode) throws ProtocolException {
        List<CheckpointState> affected = new ArrayList<>();
        for (CheckpointState checkpoint : checkpoints.values()) {
            if (!checkpoint.prefix.equals(prefix)) continue;
            boolean belongsToOlderOwner = checkpoint.ownerEpoch < ownerEpoch;
            boolean newerThanRestoredCheckpoint = mode == ClaimMode.RESUME
                && checkpoint.ownerEpoch == ownerEpoch && checkpoint.ownerSlot == ownerSlot
                && checkpoint.checkpointId > restoredCheckpoint;
            if (belongsToOlderOwner || newerThanRestoredCheckpoint) affected.add(checkpoint);
        }
        affected.sort(Comparator.comparingLong((CheckpointState state) -> state.ownerEpoch)
            .thenComparingInt(state -> state.ownerSlot).thenComparingLong(state -> state.checkpointId));
        for (CheckpointState checkpoint : affected) {
            if (checkpoint.decision == null) appendCheckpointDecision(checkpoint, Decision.ROLLBACK);
            completeCheckpoint(checkpoint);
        }
    }

    private void appendCheckpointDecision(CheckpointState checkpoint, Decision decision)
        throws ProtocolException {
        Map<String, String> properties = baseProperties(checkpoint.prefix);
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE, RECORD_DECISION);
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_EPOCH,
            Long.toString(checkpoint.ownerEpoch));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_EPOCH,
            Long.toString(checkpoint.claimantEpoch));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_SLOT,
            Integer.toString(checkpoint.ownerSlot));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CHECKPOINT,
            Long.toString(checkpoint.checkpointId));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_DECISION, decision.name());
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_COUNT,
            Integer.toString(checkpoint.handles.size()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_DIGEST,
            RecoverableTransactionCheckpoint.computeDigest(checkpoint.handles.keySet()));
        appendOutcomeRecord(properties);
    }
    private void validateDecisionContents(CheckpointState checkpoint) throws ProtocolException {
        if (checkpoint.decision == null) return;
        if (checkpoint.handles.size() > checkpoint.expectedCount) {
            throw new ProtocolException(ErrorCode.CHECKPOINT_MISMATCH,
                "durable checkpoint contains more handles than its immutable decision");
        }
        if (checkpoint.handles.size() == checkpoint.expectedCount
            && !RecoverableTransactionCheckpoint.computeDigest(checkpoint.handles.keySet())
                .equals(checkpoint.expectedDigest)) {
            throw new ProtocolException(ErrorCode.CHECKPOINT_MISMATCH,
                "durable checkpoint handles do not match its immutable decision");
        }
        for (HandleState handle : checkpoint.handles.values()) {
            if (checkpoint.decision == Decision.COMMIT && handle.outcome == Outcome.ROLLED_BACK
                || checkpoint.decision == Decision.ROLLBACK && handle.outcome == Outcome.COMMITTED) {
                throw new ProtocolException(ErrorCode.DECISION_CONFLICT,
                    "per-handle completion conflicts with the immutable checkpoint decision");
            }
        }
    }

    private void applyCompletionEvidence(String handleId, Outcome outcome, long completionOffset)
        throws ProtocolException {
        if (handleId == null || handleId.isEmpty()) {
            throw new ProtocolException(ErrorCode.STORAGE_ERROR, "completion record has no handle id");
        }
        HandleState state = handles.get(handleId);
        if (state != null) {
            CheckpointState checkpoint = checkpoints.get(checkpointKey(state.handle));
            if (checkpoint != null && checkpoint.decision != null
                && (checkpoint.decision == Decision.COMMIT) != (outcome == Outcome.COMMITTED)) {
                throw new ProtocolException(ErrorCode.DECISION_CONFLICT,
                    "per-handle completion conflicts with the immutable checkpoint decision");
            }
        }
        CompletionEvidence candidate = new CompletionEvidence(outcome, completionOffset);
        CompletionEvidence existing = completionEvidence.putIfAbsent(handleId, candidate);
        CompletionEvidence effective = existing == null ? candidate : existing;
        if (effective.outcome != outcome) {
            throw new ProtocolException(ErrorCode.DECISION_CONFLICT,
                "prepared handle has conflicting durable completion evidence");
        }
        effective.completionOffset = Math.max(effective.completionOffset, completionOffset);
        if (state != null) {
            state.outcome = outcome;
            state.completionOffset = effective.completionOffset;
        }
    }

    private boolean isFenced(PreparedTransactionHandle handle) {
        OwnerState owner = owners.get(handle.getTransactionalIdPrefix());
        if (owner == null || handle.getOwnerEpoch() < owner.ownerEpoch) return true;
        ClaimState claim = owner.claims.get(handle.getOwnerSlot());
        return claim != null && handle.getClaimantEpoch() < claim.claimantEpoch
            && handle.getCheckpointId() > claim.restoredCheckpointId;
    }

    private void validateCurrentOwner(RecoverableTransactionOwner owner) throws ProtocolException {
        OwnerState state = owners.get(owner.getTransactionalIdPrefix());
        if (state == null || owner.getOwnerEpoch() != state.ownerEpoch) {
            throw new ProtocolException(ErrorCode.STALE_OWNER_EPOCH, "owner epoch is fenced");
        }
        if (!owner.getProducerGroup().equals(state.producerGroup)) {
            throw new ProtocolException(ErrorCode.OWNERSHIP_CONFLICT,
                "producer group does not own the transactional id prefix");
        }
        ClaimState claim = state.claims.get(owner.getOwnerSlot());
        if (claim == null || owner.getClaimantEpoch() != claim.claimantEpoch
            || !owner.getClaimantIdentity().equals(claim.identity)
            || owner.getRestoredCheckpointId() != claim.restoredCheckpointId) {
            throw new ProtocolException(ErrorCode.STALE_CLAIMANT_EPOCH, "claimant epoch is fenced");
        }
    }

    private void validateFinalizeOwner(RecoverableTransactionOwner owner,
        RecoverableTransactionCheckpoint checkpoint) throws ProtocolException {
        if (!owner.getBrokerName().equals(checkpoint.getBrokerName())
            || !owner.getBrokerAddress().equals(checkpoint.getBrokerAddress())
            || !owner.getProducerGroup().equals(checkpoint.getProducerGroup())
            || !owner.getTransactionalIdPrefix().equals(checkpoint.getTransactionalIdPrefix())
            || owner.getOwnerEpoch() != checkpoint.getOwnerEpoch()
            || owner.getOwnerSlot() != checkpoint.getOwnerSlot()) {
            throw new ProtocolException(ErrorCode.STALE_OWNER_EPOCH, "checkpoint is owned by another owner");
        }
        OwnerState state = owners.get(owner.getTransactionalIdPrefix());
        if (state == null || owner.getOwnerEpoch() != state.ownerEpoch) {
            throw new ProtocolException(ErrorCode.STALE_OWNER_EPOCH, "owner epoch is fenced");
        }
        if (!owner.getProducerGroup().equals(state.producerGroup)) {
            throw new ProtocolException(ErrorCode.OWNERSHIP_CONFLICT,
                "producer group does not own the transactional id prefix");
        }
        ClaimState active = state.claims.get(owner.getOwnerSlot());
        if (active == null) {
            throw new ProtocolException(ErrorCode.STALE_CLAIMANT_EPOCH, "owner slot is not claimed");
        }
        if (owner.getClaimantEpoch() == active.claimantEpoch) {
            if (!owner.getClaimantIdentity().equals(active.identity)
                || owner.getRestoredCheckpointId() != active.restoredCheckpointId) {
                throw new ProtocolException(ErrorCode.STALE_CLAIMANT_EPOCH,
                    "current claimant token does not match durable ownership");
            }
            return;
        }
        if (checkpoint.getCheckpointId() > active.restoredCheckpointId) {
            throw new ProtocolException(ErrorCode.STALE_CLAIMANT_EPOCH,
                "checkpoint was not adopted by the current claimant");
        }
    }

    private CheckpointState requireMatchingCheckpoint(RecoverableTransactionCheckpoint checkpoint)
        throws ProtocolException {
        CheckpointState state = checkpoints.get(checkpointKey(checkpoint.getTransactionalIdPrefix(),
            checkpoint.getOwnerEpoch(), checkpoint.getClaimantEpoch(), checkpoint.getOwnerSlot(),
            checkpoint.getCheckpointId()));
        if (state == null) throw new ProtocolException(ErrorCode.NOT_FOUND, "checkpoint was not found");
        List<String> durableIds = new ArrayList<>(state.handles.keySet());
        String durableDigest = RecoverableTransactionCheckpoint.computeDigest(durableIds);
        if (state.handles.size() != checkpoint.getPreparedCount()
            || !durableDigest.equals(checkpoint.getDigest())) {
            throw new ProtocolException(ErrorCode.CHECKPOINT_MISMATCH,
                "prepared count or digest does not match durable broker state");
        }
        return state;
    }

    private PreparedTransactionHandle recordPreparedLocked(PreparedTransactionHandle handle)
        throws ProtocolException {
        CheckpointState checkpoint = checkpoints.computeIfAbsent(checkpointKey(handle),
            ignored -> new CheckpointState(handle.getTransactionalIdPrefix(), handle.getOwnerEpoch(),
                handle.getClaimantEpoch(), handle.getOwnerSlot(), handle.getCheckpointId()));
        HandleState existing = handles.get(handle.getHandleId());
        if (existing != null && (!existing.handle.getPayloadDigest().equals(handle.getPayloadDigest())
            || !existing.handle.getProducerGroup().equals(handle.getProducerGroup())
            || !existing.handle.getTransactionalIdPrefix().equals(handle.getTransactionalIdPrefix())
            || existing.handle.getOwnerEpoch() != handle.getOwnerEpoch()
            || existing.handle.getOwnerSlot() != handle.getOwnerSlot()
            || existing.handle.getCheckpointId() != handle.getCheckpointId()
            || existing.handle.getMessageSequence() != handle.getMessageSequence()
            || !existing.handle.getTopic().equals(handle.getTopic())
            || existing.handle.getQueueId() != handle.getQueueId())) {
            throw new ProtocolException(ErrorCode.HANDLE_COLLISION, "durable handle collision");
        }
        if (existing == null && checkpoint.decision != null
            && checkpoint.handles.size() >= checkpoint.expectedCount) {
            throw new ProtocolException(ErrorCode.CHECKPOINT_MISMATCH,
                "cannot add a handle after the checkpoint decision is complete");
        }
        HandleState newState = new HandleState(handle);
        HandleState raced = handles.putIfAbsent(handle.getHandleId(), newState);
        HandleState effective = raced == null ? newState : raced;
        boolean added = checkpoint.handles.putIfAbsent(handle.getHandleId(), effective) == null;
        effective.currentHalfCommitLogOffset = Math.max(effective.currentHalfCommitLogOffset,
            handle.getHalfMessageCommitLogOffset());
        try {
            CompletionEvidence evidence = completionEvidence.get(handle.getHandleId());
            if (evidence != null) {
                applyCompletionEvidence(handle.getHandleId(), evidence.outcome, evidence.completionOffset);
            }
            validateDecisionContents(checkpoint);
        } catch (ProtocolException e) {
            if (added) checkpoint.handles.remove(handle.getHandleId(), effective);
            if (raced == null) handles.remove(handle.getHandleId(), newState);
            throw e;
        }
        return effective.handle;
    }

    private RecoverableTransactionResult result(CheckpointState state, boolean idempotent) {
        int completed = 0;
        List<PreparedTransactionHandle> pending = new ArrayList<>();
        for (HandleState handle : state.handles.values()) {
            boolean done = state.decision == Decision.COMMIT && handle.outcome == Outcome.COMMITTED
                || state.decision == Decision.ROLLBACK && handle.outcome == Outcome.ROLLED_BACK;
            if (done) {
                completed++;
            } else {
                pending.add(handle.handle);
            }
        }
        Outcome outcome;
        if (state.decision == null) {
            outcome = Outcome.PENDING;
        } else if (completed != state.handles.size()
            || state.expectedCount != state.handles.size()) {
            outcome = Outcome.COMMITTING;
        } else {
            outcome = state.decision == Decision.COMMIT ? Outcome.COMMITTED : Outcome.ROLLED_BACK;
        }
        return new RecoverableTransactionResult(state.prefix, state.ownerEpoch, state.ownerSlot,
            state.checkpointId, state.decision, outcome, state.handles.size(), completed, idempotent, pending);
    }

    private RecoverableTransactionOwner ownerResult(String prefix, int ownerSlot) {
        OwnerState owner = owners.get(prefix);
        ClaimState claim = owner.claims.get(ownerSlot);
        return new RecoverableTransactionOwner(
            org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.CURRENT_VERSION,
            brokerController.getBrokerConfig().getBrokerClusterName(),
            brokerController.getBrokerConfig().getBrokerName(), brokerController.getBrokerAddr(),
            owner.producerGroup, prefix, claim.identity, ownerSlot, owner.ownerEpoch, claim.claimantEpoch,
            claim.restoredCheckpointId);
    }

    private PreparedTransactionHandle fromDispatchRequest(DispatchRequest request) {
        Map<String, String> p = request.getPropertiesMap();
        return new PreparedTransactionHandle(intProperty(p,
            MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_VERSION),
            brokerController.getBrokerConfig().getBrokerClusterName(),
            brokerController.getBrokerConfig().getBrokerName(), brokerController.getBrokerAddr(),
            p.get(MessageConst.PROPERTY_REAL_TOPIC), intProperty(p, MessageConst.PROPERTY_REAL_QUEUE_ID),
            p.get(MessageConst.PROPERTY_PRODUCER_GROUP),
            p.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PREFIX),
            longProperty(p, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_EPOCH),
            longProperty(p, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_EPOCH),
            intProperty(p, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_SLOT),
            longProperty(p, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CHECKPOINT),
            longProperty(p, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_SEQUENCE),
            p.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_HANDLE),
            p.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PAYLOAD_DIGEST),
            MessageDecoder.createMessageId(brokerController.getStoreHost(), request.getCommitLogOffset()),
            p.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX), request.getQueueId(),
            request.getConsumeQueueOffset(), request.getCommitLogOffset());
    }

    private PreparedTransactionHandle fromHalfMessage(MessageExt message) {
        Map<String, String> p = message.getProperties();
        return new PreparedTransactionHandle(intProperty(p,
            MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_VERSION),
            brokerController.getBrokerConfig().getBrokerClusterName(),
            brokerController.getBrokerConfig().getBrokerName(), brokerController.getBrokerAddr(),
            p.get(MessageConst.PROPERTY_REAL_TOPIC), intProperty(p, MessageConst.PROPERTY_REAL_QUEUE_ID),
            p.get(MessageConst.PROPERTY_PRODUCER_GROUP),
            p.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PREFIX),
            longProperty(p, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_EPOCH),
            longProperty(p, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_EPOCH),
            intProperty(p, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_SLOT),
            longProperty(p, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CHECKPOINT),
            longProperty(p, MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_SEQUENCE),
            p.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_HANDLE),
            p.get(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PAYLOAD_DIGEST), message.getMsgId(),
            p.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX), message.getQueueId(),
            message.getQueueOffset(), message.getCommitLogOffset());
    }

    private Map<String, String> handleProperties(PreparedTransactionHandle handle) {
        Map<String, String> result = baseProperties(handle.getTransactionalIdPrefix());
        result.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_HANDLE, handle.getHandleId());
        result.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_EPOCH,
            Long.toString(handle.getOwnerEpoch()));
        result.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_EPOCH,
            Long.toString(handle.getClaimantEpoch()));
        result.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_SLOT,
            Integer.toString(handle.getOwnerSlot()));
        result.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CHECKPOINT,
            Long.toString(handle.getCheckpointId()));
        result.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_SEQUENCE,
            Long.toString(handle.getMessageSequence()));
        result.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PAYLOAD_DIGEST, handle.getPayloadDigest());
        return result;
    }

    private Map<String, String> baseProperties(String prefix) {
        Map<String, String> result = new HashMap<>();
        result.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION, "true");
        result.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_VERSION,
            Integer.toString(org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.CURRENT_VERSION));
        result.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PREFIX, prefix);
        return result;
    }

    private String checkpointKey(PreparedTransactionHandle handle) {
        return checkpointKey(handle.getTransactionalIdPrefix(), handle.getOwnerEpoch(),
            handle.getClaimantEpoch(), handle.getOwnerSlot(), handle.getCheckpointId());
    }

    private String checkpointKey(String prefix, long ownerEpoch, long claimantEpoch, int ownerSlot,
        long checkpointId) {
        return prefix + '\u0001' + ownerEpoch + '\u0001' + claimantEpoch + '\u0001' + ownerSlot
            + '\u0001' + checkpointId;
    }

    private ReentrantLock lockFor(String prefix) {
        if (prefix == null) throw new IllegalArgumentException("transactionalIdPrefix must not be null");
        int hash = prefix.hashCode();
        return locks[(hash ^ (hash >>> 16)) & (LOCK_COUNT - 1)];
    }

    private static int intProperty(Map<String, String> properties, String key) {
        return Integer.parseInt(properties.get(key));
    }

    private static long longProperty(Map<String, String> properties, String key) {
        return Long.parseLong(properties.get(key));
    }

    private static void requireVersion(int version) throws ProtocolException {
        if (version != org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.CURRENT_VERSION) {
            throw new ProtocolException(ErrorCode.UNSUPPORTED_VERSION,
                "unsupported recoverable transaction protocol version " + version);
        }
    }

    public enum CheckResolution {
        CLASSIC,
        PENDING,
        COMMITTED,
        ROLLED_BACK
    }

    public enum FailurePoint {
        DECISION_PERSISTED,
        FINAL_MESSAGE_APPENDED,
        RUNTIME_INDEX_UPDATED,
        RESPONSE_WRITTEN
    }

    @FunctionalInterface
    public interface FailureHook {
        FailureHook NOOP = (point, checkpointId, handleId) -> { };
        void onFailure(FailurePoint point, long checkpointId, String handleId);
    }

    public static class ProtocolException extends Exception {
        private static final long serialVersionUID = -3414915858264448437L;
        private final ErrorCode errorCode;

        public ProtocolException(ErrorCode errorCode, String message) {
            super(message);
            this.errorCode = errorCode;
        }

        public ProtocolException(ErrorCode errorCode, String message, Throwable cause) {
            super(message, cause);
            this.errorCode = errorCode;
        }

        public ErrorCode getErrorCode() {
            return errorCode;
        }
    }

    private static final class OwnerState {
        private long ownerEpoch;
        private long nextClaimantEpoch;
        private String producerGroup;
        private String namespaceJobId;
        private final Map<Integer, ClaimState> claims = new HashMap<>();
    }

    private static final class ClaimState {
        private final String identity;
        private final long claimantEpoch;
        private final long restoredCheckpointId;
        private final ClaimMode mode;

        private ClaimState(String identity, long claimantEpoch, long restoredCheckpointId, ClaimMode mode) {
            this.identity = identity;
            this.claimantEpoch = claimantEpoch;
            this.restoredCheckpointId = restoredCheckpointId;
            this.mode = mode;
        }
    }

    private static final class CompletionEvidence {
        private final Outcome outcome;
        private volatile long completionOffset;

        private CompletionEvidence(Outcome outcome, long completionOffset) {
            this.outcome = outcome;
            this.completionOffset = completionOffset;
        }
    }

    private static final class HandleState {
        private final PreparedTransactionHandle handle;
        private volatile Outcome outcome = Outcome.PENDING;
        private volatile long completionOffset = -1;
        private volatile long currentHalfCommitLogOffset;

        private HandleState(PreparedTransactionHandle handle) {
            this.handle = handle;
            this.currentHalfCommitLogOffset = handle.getHalfMessageCommitLogOffset();
        }
    }

    private static final class CheckpointState {
        private final String prefix;
        private final long ownerEpoch;
        private final long claimantEpoch;
        private final int ownerSlot;
        private final long checkpointId;
        private final Map<String, HandleState> handles = new LinkedHashMap<>();
        private Decision decision;
        private int expectedCount;
        private String expectedDigest;

        private CheckpointState(String prefix, long ownerEpoch, long claimantEpoch, int ownerSlot,
            long checkpointId) {
            this.prefix = prefix;
            this.ownerEpoch = ownerEpoch;
            this.claimantEpoch = claimantEpoch;
            this.ownerSlot = ownerSlot;
            this.checkpointId = checkpointId;
        }
    }
}
