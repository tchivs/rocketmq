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
package org.apache.rocketmq.client.producer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RecoverableTransactionException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.transaction.PreparedTransactionHandle;
import org.apache.rocketmq.common.transaction.RecoverableTransactionCapability;
import org.apache.rocketmq.common.transaction.RecoverableTransactionCheckpoint;
import org.apache.rocketmq.common.transaction.RecoverableTransactionOwner;
import org.apache.rocketmq.common.transaction.RecoverableTransactionOwnerClaim;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.Decision;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.ErrorCode;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.Operation;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.Outcome;
import org.apache.rocketmq.common.transaction.RecoverableTransactionResult;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.RecoverableTransactionHandleData;
import org.apache.rocketmq.remoting.protocol.body.RecoverableTransactionRequestBody;
import org.apache.rocketmq.remoting.protocol.body.RecoverableTransactionResponseBody;

/**
 * Public producer API for broker-backed transactions that survive producer and broker failure.
 *
 * <p>The API deliberately exchanges only immutable public DTOs. A restored process may deserialize a prepared
 * handle, start a new producer instance, negotiate the broker capability, reclaim ownership, and safely retry the
 * same synchronous decision.</p>
 */
public class RecoverableTransactionProducer {
    private final DefaultMQProducer delegate;

    public RecoverableTransactionProducer(String producerGroup) {
        this.delegate = new DefaultMQProducer(producerGroup);
    }

    public RecoverableTransactionProducer(String producerGroup, RPCHook rpcHook) {
        this.delegate = new DefaultMQProducer(producerGroup, rpcHook);
    }

    public void setNamesrvAddr(String namesrvAddr) {
        delegate.setNamesrvAddr(namesrvAddr);
    }

    public void setVipChannelEnabled(boolean vipChannelEnabled) {
        delegate.setVipChannelEnabled(vipChannelEnabled);
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        delegate.setSendMsgTimeout(sendMsgTimeout);
    }

    public void setInstanceName(String instanceName) {
        delegate.setInstanceName(instanceName);
    }

    public void start() throws MQClientException {
        delegate.start();
    }

    public void shutdown() {
        delegate.shutdown();
    }

    /** Returns one verified capability for every broker that currently writes the topic. */
    public List<RecoverableTransactionCapability> negotiateCapabilities(String topic)
        throws MQClientException, RemotingException, InterruptedException {
        List<MessageQueue> queues = delegate.fetchPublishMessageQueues(topic);
        Set<String> seen = new HashSet<>();
        List<RecoverableTransactionCapability> result = new ArrayList<>();
        for (MessageQueue queue : queues) {
            if (!seen.add(queue.getBrokerName())) continue;
            String address = delegate.getDefaultMQProducerImpl().getMqClientFactory()
                .findBrokerAddressInPublish(queue.getBrokerName());
            if (address == null) {
                delegate.getDefaultMQProducerImpl().getMqClientFactory()
                    .updateTopicRouteInfoFromNameServer(topic);
                address = delegate.getDefaultMQProducerImpl().getMqClientFactory()
                    .findBrokerAddressInPublish(queue.getBrokerName());
            }
            if (address == null) {
                throw new MQClientException("No master address for broker " + queue.getBrokerName(), null);
            }
            RecoverableTransactionRequestBody request = request(Operation.CAPABILITY);
            RecoverableTransactionResponseBody response = invoke(address, request);
            result.add(new RecoverableTransactionCapability(response.getProtocolVersion(),
                response.getClusterName(), response.getBrokerName(), response.getBrokerAddress()));
        }
        result.sort(Comparator.comparing(RecoverableTransactionCapability::getBrokerName));
        return Collections.unmodifiableList(result);
    }

    /** Claims a fresh namespace, resumes restored state, or explicitly takes over a broker namespace. */
    public RecoverableTransactionOwner claimOwnership(RecoverableTransactionCapability capability,
        RecoverableTransactionOwnerClaim claim)
        throws RecoverableTransactionException, RemotingException, InterruptedException {
        if (capability == null || claim == null) {
            throw new IllegalArgumentException("capability and claim are required");
        }
        if (capability.getProtocolVersion() != RecoverableTransactionProtocol.CURRENT_VERSION
            || claim.getProtocolVersion() != RecoverableTransactionProtocol.CURRENT_VERSION) {
            throw new RecoverableTransactionException(ErrorCode.UNSUPPORTED_VERSION,
                "broker or claim uses an unsupported recoverable transaction version");
        }
        RecoverableTransactionRequestBody request = request(Operation.CLAIM);
        request.setProducerGroup(delegate.getProducerGroup());
        request.setTransactionalIdPrefix(claim.getTransactionalIdPrefix());
        request.setJobId(claim.getJobId());
        request.setOwnerSlot(claim.getOwnerSlot());
        request.setAttemptNumber(claim.getAttemptNumber());
        request.setRestoredCheckpointId(claim.getRestoredCheckpointId());
        request.setExpectedOwnerEpoch(claim.getExpectedOwnerEpoch());
        request.setClaimMode(claim.getMode().name());
        RecoverableTransactionResponseBody response = invoke(capability.getBrokerAddress(), request);
        return new RecoverableTransactionOwner(response.getProtocolVersion(), response.getClusterName(),
            response.getBrokerName(), response.getBrokerAddress(), response.getProducerGroup(),
            response.getTransactionalIdPrefix(), response.getClaimantIdentity(), response.getOwnerSlot(),
            response.getOwnerEpoch(), response.getClaimantEpoch(), response.getRestoredCheckpointId());
    }

    /** Prepares one invisible half message under the supplied broker-issued owner token. */
    public PreparedTransactionHandle prepareMessage(Message message, RecoverableTransactionOwner owner,
        long checkpointId, long messageSequence)
        throws MQClientException, RemotingException, InterruptedException {
        if (message == null || message.getBody() == null || owner == null
            || checkpointId < 0 || messageSequence < 0) {
            throw new IllegalArgumentException("message, owner, checkpointId and messageSequence are required");
        }
        if (owner.getProtocolVersion() != RecoverableTransactionProtocol.CURRENT_VERSION) {
            throw new RecoverableTransactionException(ErrorCode.UNSUPPORTED_VERSION,
                "owner uses an unsupported recoverable transaction version");
        }
        if (!delegate.getProducerGroup().equals(owner.getProducerGroup())) {
            throw new RecoverableTransactionException(ErrorCode.OWNERSHIP_CONFLICT,
                "producer group does not match the broker-issued owner");
        }
        List<MessageQueue> queues = new ArrayList<>();
        for (MessageQueue queue : delegate.fetchPublishMessageQueues(message.getTopic())) {
            if (owner.getBrokerName().equals(queue.getBrokerName())) queues.add(queue);
        }
        if (queues.isEmpty()) {
            throw new RecoverableTransactionException(ErrorCode.INVALID_REQUEST,
                "owner broker does not currently write topic " + message.getTopic());
        }
        queues.sort(Comparator.comparingInt(MessageQueue::getQueueId));
        MessageQueue queue = queues.get((int) Math.floorMod(messageSequence, queues.size()));
        String payloadDigest = RecoverableTransactionCheckpoint.sha256(message.getBody());
        String handleId = RecoverableTransactionCheckpoint.computeHandleId(owner.getTransactionalIdPrefix(),
            owner.getOwnerEpoch(), owner.getClaimantEpoch(), owner.getOwnerSlot(), checkpointId,
            messageSequence, payloadDigest);

        RecoverableTransactionRequestBody request = request(Operation.PREPARE);
        applyOwner(request, owner);
        request.setCheckpointId(checkpointId);
        request.setMessageSequence(messageSequence);
        request.setHandleId(handleId);
        request.setPayloadDigest(payloadDigest);
        request.setTopic(queue.getTopic());
        request.setQueueId(queue.getQueueId());
        request.setFlag(message.getFlag());
        request.setBornTimestamp(System.currentTimeMillis());
        request.setMessageBody(message.getBody());
        request.setMessageProperties(message.getProperties());
        RecoverableTransactionResponseBody response = invoke(owner.getBrokerAddress(), request);
        if (response.getHandles() == null || response.getHandles().size() != 1) {
            throw new RecoverableTransactionException(ErrorCode.STORAGE_ERROR,
                "broker returned no prepared transaction handle");
        }
        return response.getHandles().get(0).toHandle();
    }

    /** Synchronously commits every handle in one per-broker checkpoint. */
    public RecoverableTransactionResult commit(RecoverableTransactionOwner owner,
        RecoverableTransactionCheckpoint checkpoint)
        throws RecoverableTransactionException, RemotingException, InterruptedException {
        return finalizeCheckpoint(owner, checkpoint, Decision.COMMIT);
    }

    /** Synchronously rolls back every handle in one per-broker checkpoint. */
    public RecoverableTransactionResult rollback(RecoverableTransactionOwner owner,
        RecoverableTransactionCheckpoint checkpoint)
        throws RecoverableTransactionException, RemotingException, InterruptedException {
        return finalizeCheckpoint(owner, checkpoint, Decision.ROLLBACK);
    }

    /** Queries the durable checkpoint that contains the supplied stable handle. */
    public RecoverableTransactionResult queryOutcome(PreparedTransactionHandle handle)
        throws RecoverableTransactionException, RemotingException, InterruptedException {
        requireCurrentHandle(handle);
        RecoverableTransactionRequestBody request = request(Operation.QUERY);
        request.setHandleId(handle.getHandleId());
        return responseResult(invoke(handle.getBrokerAddress(), request));
    }

    /** Queries a checkpoint without requiring a live owner token. */
    public RecoverableTransactionResult queryOutcome(RecoverableTransactionCheckpoint checkpoint)
        throws RecoverableTransactionException, RemotingException, InterruptedException {
        requireCurrentCheckpoint(checkpoint);
        RecoverableTransactionRequestBody request = checkpointRequest(Operation.QUERY, checkpoint);
        return responseResult(invoke(checkpoint.getBrokerAddress(), request));
    }

    /** Lists invisible pending handles for an owner namespace on one broker. */
    public List<PreparedTransactionHandle> listPending(RecoverableTransactionCapability capability,
        String transactionalIdPrefix)
        throws RecoverableTransactionException, RemotingException, InterruptedException {
        if (capability == null || transactionalIdPrefix == null || transactionalIdPrefix.isEmpty()) {
            throw new IllegalArgumentException("capability and transactionalIdPrefix are required");
        }
        if (capability.getProtocolVersion() != RecoverableTransactionProtocol.CURRENT_VERSION) {
            throw new RecoverableTransactionException(ErrorCode.UNSUPPORTED_VERSION,
                "broker uses an unsupported recoverable transaction version");
        }
        RecoverableTransactionRequestBody request = request(Operation.LIST);
        request.setTransactionalIdPrefix(transactionalIdPrefix);
        RecoverableTransactionResponseBody response = invoke(capability.getBrokerAddress(), request);
        List<PreparedTransactionHandle> result = new ArrayList<>();
        if (response.getHandles() != null) {
            for (RecoverableTransactionHandleData data : response.getHandles()) result.add(data.toHandle());
        }
        return Collections.unmodifiableList(result);
    }

    /** Administratively aborts a still-pending checkpoint; a recorded COMMIT is never overwritten. */
    public RecoverableTransactionResult abort(RecoverableTransactionCheckpoint checkpoint)
        throws RecoverableTransactionException, RemotingException, InterruptedException {
        requireCurrentCheckpoint(checkpoint);
        RecoverableTransactionRequestBody request = checkpointRequest(Operation.ABORT, checkpoint);
        request.setDecision(Decision.ROLLBACK.name());
        return responseResult(invoke(checkpoint.getBrokerAddress(), request));
    }

    private RecoverableTransactionResult finalizeCheckpoint(RecoverableTransactionOwner owner,
        RecoverableTransactionCheckpoint checkpoint, Decision decision)
        throws RecoverableTransactionException, RemotingException, InterruptedException {
        if (owner == null) throw new IllegalArgumentException("owner is required");
        requireCurrentCheckpoint(checkpoint);
        if (owner.getProtocolVersion() != RecoverableTransactionProtocol.CURRENT_VERSION) {
            throw new RecoverableTransactionException(ErrorCode.UNSUPPORTED_VERSION,
                "owner uses an unsupported recoverable transaction version");
        }
        RecoverableTransactionRequestBody request = checkpointRequest(Operation.FINALIZE, checkpoint);
        applyOwner(request, owner);
        request.setDecision(decision.name());
        return responseResult(invoke(checkpoint.getBrokerAddress(), request));
    }

    private RecoverableTransactionRequestBody checkpointRequest(Operation operation,
        RecoverableTransactionCheckpoint checkpoint) {
        RecoverableTransactionRequestBody request = request(operation);
        request.setTransactionalIdPrefix(checkpoint.getTransactionalIdPrefix());
        request.setOwnerEpoch(checkpoint.getOwnerEpoch());
        request.setOwnerSlot(checkpoint.getOwnerSlot());
        request.setCheckpointId(checkpoint.getCheckpointId());
        request.setPreparedCount(checkpoint.getPreparedCount());
        request.setDigest(checkpoint.getDigest());
        List<RecoverableTransactionHandleData> handles = new ArrayList<>(checkpoint.getPreparedCount());
        for (PreparedTransactionHandle handle : checkpoint.getHandles()) {
            handles.add(RecoverableTransactionHandleData.from(handle));
        }
        request.setHandles(handles);
        return request;
    }

    private void applyOwner(RecoverableTransactionRequestBody request, RecoverableTransactionOwner owner) {
        request.setProducerGroup(owner.getProducerGroup());
        request.setTransactionalIdPrefix(owner.getTransactionalIdPrefix());
        request.setOwnerSlot(owner.getOwnerSlot());
        request.setOwnerEpoch(owner.getOwnerEpoch());
        request.setClaimantEpoch(owner.getClaimantEpoch());
        request.setClaimantIdentity(owner.getClaimantIdentity());
        request.setRestoredCheckpointId(owner.getRestoredCheckpointId());
    }

    private RecoverableTransactionRequestBody request(Operation operation) {
        RecoverableTransactionRequestBody request = new RecoverableTransactionRequestBody();
        request.setProtocolVersion(RecoverableTransactionProtocol.CURRENT_VERSION);
        request.setOperation(operation.name());
        return request;
    }

    private RecoverableTransactionResponseBody invoke(String brokerAddress,
        RecoverableTransactionRequestBody request)
        throws RecoverableTransactionException, RemotingException, InterruptedException {
        try {
            RecoverableTransactionResponseBody response = delegate.getDefaultMQProducerImpl()
                .getMqClientFactory().getMQClientAPIImpl()
                .recoverableTransaction(brokerAddress, request, delegate.getSendMsgTimeout());
            if (response == null) {
                throw new RecoverableTransactionException(ErrorCode.STORAGE_ERROR,
                    "broker returned an empty recoverable transaction response");
            }
            if (response.getProtocolVersion() != RecoverableTransactionProtocol.CURRENT_VERSION) {
                throw new RecoverableTransactionException(ErrorCode.UNSUPPORTED_VERSION,
                    "broker returned an unsupported recoverable transaction version");
            }
            return response;
        } catch (MQBrokerException e) {
            throw new RecoverableTransactionException(errorCode(e.getResponseCode()),
                e.getErrorMessage(), e);
        }
    }

    private RecoverableTransactionResult responseResult(RecoverableTransactionResponseBody response)
        throws RecoverableTransactionException {
        try {
            List<PreparedTransactionHandle> pending = new ArrayList<>();
            if (response.getHandles() != null) {
                for (RecoverableTransactionHandleData data : response.getHandles()) pending.add(data.toHandle());
            }
            return new RecoverableTransactionResult(response.getTransactionalIdPrefix(), response.getOwnerEpoch(),
                response.getOwnerSlot(), response.getCheckpointId(),
                response.getDecision() == null ? null : Decision.valueOf(response.getDecision()),
                Outcome.valueOf(response.getOutcome()), response.getPreparedCount(), response.getCompletedCount(),
                response.isIdempotent(), pending);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new RecoverableTransactionException(ErrorCode.STORAGE_ERROR,
                "broker returned an invalid recoverable transaction response", e);
        }
    }

    private void requireCurrentHandle(PreparedTransactionHandle handle)
        throws RecoverableTransactionException {
        if (handle == null) throw new IllegalArgumentException("handle is required");
        if (handle.getProtocolVersion() != RecoverableTransactionProtocol.CURRENT_VERSION) {
            throw new RecoverableTransactionException(ErrorCode.UNSUPPORTED_VERSION,
                "handle uses an unsupported recoverable transaction version");
        }
    }

    private void requireCurrentCheckpoint(RecoverableTransactionCheckpoint checkpoint)
        throws RecoverableTransactionException {
        if (checkpoint == null) throw new IllegalArgumentException("checkpoint is required");
        if (checkpoint.getProtocolVersion() != RecoverableTransactionProtocol.CURRENT_VERSION) {
            throw new RecoverableTransactionException(ErrorCode.UNSUPPORTED_VERSION,
                "checkpoint uses an unsupported recoverable transaction version");
        }
    }

    private ErrorCode errorCode(int responseCode) {
        switch (responseCode) {
            case ResponseCode.REQUEST_CODE_NOT_SUPPORTED: return ErrorCode.UNSUPPORTED_VERSION;
            case ResponseCode.RECOVERABLE_TRANSACTION_UNSUPPORTED_VERSION: return ErrorCode.UNSUPPORTED_VERSION;
            case ResponseCode.RECOVERABLE_TRANSACTION_INVALID_REQUEST: return ErrorCode.INVALID_REQUEST;
            case ResponseCode.RECOVERABLE_TRANSACTION_OWNERSHIP_CONFLICT: return ErrorCode.OWNERSHIP_CONFLICT;
            case ResponseCode.RECOVERABLE_TRANSACTION_STALE_OWNER_EPOCH: return ErrorCode.STALE_OWNER_EPOCH;
            case ResponseCode.RECOVERABLE_TRANSACTION_STALE_CLAIMANT_EPOCH: return ErrorCode.STALE_CLAIMANT_EPOCH;
            case ResponseCode.RECOVERABLE_TRANSACTION_CHECKPOINT_MISMATCH: return ErrorCode.CHECKPOINT_MISMATCH;
            case ResponseCode.RECOVERABLE_TRANSACTION_DECISION_CONFLICT: return ErrorCode.DECISION_CONFLICT;
            case ResponseCode.RECOVERABLE_TRANSACTION_HANDLE_COLLISION: return ErrorCode.HANDLE_COLLISION;
            case ResponseCode.RECOVERABLE_TRANSACTION_NOT_FOUND: return ErrorCode.NOT_FOUND;
            default: return ErrorCode.STORAGE_ERROR;
        }
    }
}
