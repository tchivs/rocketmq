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

import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.RecoverableTransactionOutcomeStore;
import org.apache.rocketmq.broker.transaction.RecoverableTransactionOutcomeStore.ProtocolException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.transaction.PreparedTransactionHandle;
import org.apache.rocketmq.common.transaction.RecoverableTransactionCheckpoint;
import org.apache.rocketmq.common.transaction.RecoverableTransactionOwner;
import org.apache.rocketmq.common.transaction.RecoverableTransactionOwnerClaim;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.ClaimMode;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.Decision;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.ErrorCode;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.Operation;
import org.apache.rocketmq.common.transaction.RecoverableTransactionResult;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.RecoverableTransactionHandleData;
import org.apache.rocketmq.remoting.protocol.body.RecoverableTransactionRequestBody;
import org.apache.rocketmq.remoting.protocol.body.RecoverableTransactionResponseBody;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;

/** Dedicated synchronous broker endpoint for the recoverable transaction protocol. */
public class RecoverableTransactionProcessor implements NettyRequestProcessor {
    private final BrokerController brokerController;

    public RecoverableTransactionProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        try {
            RecoverableTransactionRequestBody body = RecoverableTransactionRequestBody.decode(
                request.getBody(), RecoverableTransactionRequestBody.class);
            if (body == null || body.getOperation() == null) {
                return failure(response, ResponseCode.RECOVERABLE_TRANSACTION_INVALID_REQUEST,
                    "recoverable transaction operation is required");
            }
            if (body.getProtocolVersion() != RecoverableTransactionProtocol.CURRENT_VERSION) {
                return failure(response, ResponseCode.RECOVERABLE_TRANSACTION_UNSUPPORTED_VERSION,
                    "unsupported recoverable transaction protocol version " + body.getProtocolVersion());
            }
            Operation operation = Operation.valueOf(body.getOperation());
            RecoverableTransactionResponseBody responseBody;
            switch (operation) {
                case CAPABILITY:
                    responseBody = capability();
                    break;
                case CLAIM:
                    ensureMaster();
                    responseBody = claim(body);
                    break;
                case PREPARE:
                    ensureMaster();
                    responseBody = prepare(ctx, body);
                    break;
                case FINALIZE:
                    ensureMaster();
                    responseBody = finalizeCheckpoint(body, false);
                    break;
                case QUERY:
                    responseBody = query(body);
                    break;
                case LIST:
                    responseBody = list(body);
                    break;
                case ABORT:
                    ensureMaster();
                    responseBody = finalizeCheckpoint(body, true);
                    break;
                default:
                    throw new IllegalArgumentException("unsupported operation " + operation);
            }
            response.setCode(ResponseCode.SUCCESS);
            response.setBody(responseBody.encode());
            if (operation == Operation.FINALIZE || operation == Operation.ABORT) {
                brokerController.getRecoverableTransactionOutcomeStore().fireResponseHook(body.getCheckpointId());
            }
            return response;
        } catch (ProtocolException e) {
            return failure(response, responseCode(e.getErrorCode()), e.getMessage());
        } catch (IllegalArgumentException | NullPointerException e) {
            return failure(response, ResponseCode.RECOVERABLE_TRANSACTION_INVALID_REQUEST, e.getMessage());
        }
    }

    private RecoverableTransactionResponseBody capability() {
        return baseResponse();
    }

    private RecoverableTransactionResponseBody claim(RecoverableTransactionRequestBody body)
        throws ProtocolException {
        RecoverableTransactionOwnerClaim claim = new RecoverableTransactionOwnerClaim(body.getProtocolVersion(),
            body.getTransactionalIdPrefix(), body.getJobId(), body.getOwnerSlot(), body.getAttemptNumber(),
            body.getRestoredCheckpointId(), body.getExpectedOwnerEpoch(), ClaimMode.valueOf(body.getClaimMode()));
        RecoverableTransactionOwner owner = brokerController.getRecoverableTransactionOutcomeStore()
            .claim(body.getProducerGroup(), claim);
        RecoverableTransactionResponseBody result = baseResponse();
        result.setProducerGroup(owner.getProducerGroup());
        result.setTransactionalIdPrefix(owner.getTransactionalIdPrefix());
        result.setClaimantIdentity(owner.getClaimantIdentity());
        result.setOwnerSlot(owner.getOwnerSlot());
        result.setOwnerEpoch(owner.getOwnerEpoch());
        result.setClaimantEpoch(owner.getClaimantEpoch());
        result.setRestoredCheckpointId(owner.getRestoredCheckpointId());
        return result;
    }

    private RecoverableTransactionResponseBody prepare(ChannelHandlerContext ctx,
        RecoverableTransactionRequestBody body) throws ProtocolException {
        if (body.getMessageBody() == null || body.getTopic() == null || body.getProducerGroup() == null
            || body.getHandleId() == null || body.getPayloadDigest() == null || body.getQueueId() < 0
            || body.getCheckpointId() < 0 || body.getMessageSequence() < 0) {
            throw new IllegalArgumentException("invalid prepare request");
        }
        if (TopicValidator.isNotAllowedSendTopic(body.getTopic())) {
            throw new IllegalArgumentException("sending recoverable messages to protected topics is forbidden");
        }
        String expectedPayloadDigest = RecoverableTransactionCheckpoint.sha256(body.getMessageBody());
        if (!expectedPayloadDigest.equals(body.getPayloadDigest())) {
            throw new IllegalArgumentException("payload digest does not match the message body");
        }
        String expectedHandleId = RecoverableTransactionCheckpoint.computeHandleId(
            body.getTransactionalIdPrefix(), body.getOwnerEpoch(), body.getClaimantEpoch(),
            body.getOwnerSlot(), body.getCheckpointId(), body.getMessageSequence(), expectedPayloadDigest);
        if (!expectedHandleId.equals(body.getHandleId())) {
            throw new IllegalArgumentException("handle id does not match the prepared transaction identity");
        }
        RecoverableTransactionOwner owner = requestOwner(body);
        RecoverableTransactionOutcomeStore store = brokerController.getRecoverableTransactionOutcomeStore();
        PreparedTransactionHandle existing = store.validatePrepare(owner, body.getHandleId(),
            body.getPayloadDigest(), body.getCheckpointId(), body.getMessageSequence(), body.getTopic(),
            body.getQueueId());
        if (existing != null) return handleResponse(existing);

        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(body.getTopic());
        if (topicConfig == null || body.getQueueId() >= topicConfig.getWriteQueueNums()) {
            throw new IllegalArgumentException("topic or target queue does not exist");
        }
        MessageExtBrokerInner message = new MessageExtBrokerInner();
        message.setTopic(body.getTopic());
        message.setQueueId(body.getQueueId());
        message.setBody(body.getMessageBody());
        message.setFlag(body.getFlag());
        message.setBornTimestamp(body.getBornTimestamp() > 0 ? body.getBornTimestamp() : System.currentTimeMillis());
        message.setBornHost(ctx.channel().remoteAddress());
        message.setStoreHost(brokerController.getStoreHost());
        message.setWaitStoreMsgOK(true);
        message.setSysFlag(MessageSysFlag.TRANSACTION_NOT_TYPE);
        Map<String, String> properties = body.getMessageProperties() == null
            ? new HashMap<>() : new HashMap<>(body.getMessageProperties());
        properties.put(MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        properties.put(MessageConst.PROPERTY_PRODUCER_GROUP, body.getProducerGroup());
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION, "true");
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_VERSION,
            Integer.toString(body.getProtocolVersion()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PREFIX, body.getTransactionalIdPrefix());
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_EPOCH,
            Long.toString(body.getOwnerEpoch()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CLAIMANT_EPOCH,
            Long.toString(body.getClaimantEpoch()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_OWNER_SLOT,
            Integer.toString(body.getOwnerSlot()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_CHECKPOINT,
            Long.toString(body.getCheckpointId()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_SEQUENCE,
            Long.toString(body.getMessageSequence()));
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_HANDLE, body.getHandleId());
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_PAYLOAD_DIGEST, body.getPayloadDigest());
        properties.put(MessageConst.PROPERTY_RECOVERABLE_TRANSACTION_RECORD_TYPE, "PREPARED");
        MessageAccessor.setProperties(message, properties);
        MessageClientIDSetter.setUniqID(message);
        message.setTransactionId(MessageClientIDSetter.getUniqID(message));
        message.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));

        PutMessageResult putResult = brokerController.getTransactionalMessageService().prepareMessage(message);
        if (putResult == null || !putResult.isOk()) {
            throw new ProtocolException(ErrorCode.STORAGE_ERROR, "half-message append failed");
        }
        PreparedTransactionHandle handle = new PreparedTransactionHandle(body.getProtocolVersion(),
            brokerController.getBrokerConfig().getBrokerClusterName(),
            brokerController.getBrokerConfig().getBrokerName(), brokerController.getBrokerAddr(), body.getTopic(),
            body.getQueueId(), body.getProducerGroup(), body.getTransactionalIdPrefix(), body.getOwnerEpoch(),
            body.getClaimantEpoch(), body.getOwnerSlot(), body.getCheckpointId(), body.getMessageSequence(),
            body.getHandleId(), body.getPayloadDigest(), putResult.getAppendMessageResult().getMsgId(),
            MessageClientIDSetter.getUniqID(message), 0, putResult.getAppendMessageResult().getLogicsOffset(),
            putResult.getAppendMessageResult().getWroteOffset());
        handle = store.recordPrepared(owner, handle);
        if (putResult.getPutMessageStatus() != PutMessageStatus.PUT_OK) {
            throw new ProtocolException(ErrorCode.STORAGE_ERROR,
                "half-message append acknowledgement was ambiguous; retry the same handle id");
        }
        return handleResponse(handle);
    }

    private RecoverableTransactionResponseBody finalizeCheckpoint(RecoverableTransactionRequestBody body,
        boolean abort) throws ProtocolException {
        RecoverableTransactionCheckpoint checkpoint = requestCheckpoint(body);
        if (!checkpoint.getDigest().equals(body.getDigest())
            || checkpoint.getPreparedCount() != body.getPreparedCount()) {
            throw new ProtocolException(ErrorCode.CHECKPOINT_MISMATCH,
                "request count or digest does not describe the supplied handles");
        }
        RecoverableTransactionResult transactionResult = abort
            ? brokerController.getRecoverableTransactionOutcomeStore().abort(checkpoint)
            : brokerController.getRecoverableTransactionOutcomeStore().finalizeCheckpoint(
                requestOwner(body), checkpoint, Decision.valueOf(body.getDecision()));
        return resultResponse(transactionResult);
    }

    private RecoverableTransactionResponseBody query(RecoverableTransactionRequestBody body)
        throws ProtocolException {
        if (body.getHandleId() != null && body.getHandleId().isEmpty()) {
            throw new IllegalArgumentException("handle id must not be empty");
        }
        RecoverableTransactionResult result = body.getHandleId() != null
            ? brokerController.getRecoverableTransactionOutcomeStore().query(body.getHandleId())
            : brokerController.getRecoverableTransactionOutcomeStore().query(requestCheckpoint(body));
        return resultResponse(result);
    }

    private RecoverableTransactionResponseBody list(RecoverableTransactionRequestBody body) {
        if (body.getTransactionalIdPrefix() == null || body.getTransactionalIdPrefix().isEmpty()) {
            throw new IllegalArgumentException("transactional id prefix is required");
        }
        List<PreparedTransactionHandle> pending = brokerController.getRecoverableTransactionOutcomeStore()
            .listPending(body.getTransactionalIdPrefix());
        RecoverableTransactionResponseBody result = baseResponse();
        result.setTransactionalIdPrefix(body.getTransactionalIdPrefix());
        List<RecoverableTransactionHandleData> wireHandles = new ArrayList<>(pending.size());
        for (PreparedTransactionHandle handle : pending) wireHandles.add(RecoverableTransactionHandleData.from(handle));
        result.setHandles(wireHandles);
        return result;
    }

    private RecoverableTransactionCheckpoint requestCheckpoint(RecoverableTransactionRequestBody body) {
        if (body.getHandles() == null || body.getHandles().isEmpty()) {
            throw new IllegalArgumentException("checkpoint handles are required");
        }
        List<PreparedTransactionHandle> handles = new ArrayList<>(body.getHandles().size());
        for (RecoverableTransactionHandleData data : body.getHandles()) handles.add(data.toHandle());
        return new RecoverableTransactionCheckpoint(handles);
    }

    private RecoverableTransactionOwner requestOwner(RecoverableTransactionRequestBody body) {
        return new RecoverableTransactionOwner(body.getProtocolVersion(),
            brokerController.getBrokerConfig().getBrokerClusterName(),
            brokerController.getBrokerConfig().getBrokerName(), brokerController.getBrokerAddr(),
            body.getProducerGroup(), body.getTransactionalIdPrefix(), body.getClaimantIdentity(),
            body.getOwnerSlot(), body.getOwnerEpoch(), body.getClaimantEpoch(), body.getRestoredCheckpointId());
    }

    private RecoverableTransactionResponseBody baseResponse() {
        RecoverableTransactionResponseBody result = new RecoverableTransactionResponseBody();
        result.setProtocolVersion(RecoverableTransactionProtocol.CURRENT_VERSION);
        result.setClusterName(brokerController.getBrokerConfig().getBrokerClusterName());
        result.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
        result.setBrokerAddress(brokerController.getBrokerAddr());
        return result;
    }

    private RecoverableTransactionResponseBody handleResponse(PreparedTransactionHandle handle) {
        RecoverableTransactionResponseBody result = baseResponse();
        List<RecoverableTransactionHandleData> handles = new ArrayList<>(1);
        handles.add(RecoverableTransactionHandleData.from(handle));
        result.setHandles(handles);
        return result;
    }

    private RecoverableTransactionResponseBody resultResponse(RecoverableTransactionResult transaction) {
        RecoverableTransactionResponseBody result = baseResponse();
        result.setTransactionalIdPrefix(transaction.getTransactionalIdPrefix());
        result.setOwnerEpoch(transaction.getOwnerEpoch());
        result.setOwnerSlot(transaction.getOwnerSlot());
        result.setCheckpointId(transaction.getCheckpointId());
        result.setDecision(transaction.getDecision() == null ? null : transaction.getDecision().name());
        result.setOutcome(transaction.getOutcome().name());
        result.setPreparedCount(transaction.getPreparedCount());
        result.setCompletedCount(transaction.getCompletedCount());
        result.setIdempotent(transaction.isIdempotent());
        List<RecoverableTransactionHandleData> pending = new ArrayList<>();
        for (PreparedTransactionHandle handle : transaction.getPendingHandles()) {
            pending.add(RecoverableTransactionHandleData.from(handle));
        }
        result.setHandles(pending);
        return result;
    }

    private void ensureMaster() throws ProtocolException {
        if (brokerController.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
            throw new ProtocolException(ErrorCode.STORAGE_ERROR,
                "recoverable transaction mutations require the master broker");
        }
    }

    private RemotingCommand failure(RemotingCommand response, int code, String message) {
        response.setCode(code);
        response.setRemark(message == null ? "recoverable transaction request failed" : message);
        return response;
    }

    private int responseCode(ErrorCode errorCode) {
        switch (errorCode) {
            case UNSUPPORTED_VERSION: return ResponseCode.RECOVERABLE_TRANSACTION_UNSUPPORTED_VERSION;
            case INVALID_REQUEST: return ResponseCode.RECOVERABLE_TRANSACTION_INVALID_REQUEST;
            case OWNERSHIP_CONFLICT: return ResponseCode.RECOVERABLE_TRANSACTION_OWNERSHIP_CONFLICT;
            case STALE_OWNER_EPOCH: return ResponseCode.RECOVERABLE_TRANSACTION_STALE_OWNER_EPOCH;
            case STALE_CLAIMANT_EPOCH: return ResponseCode.RECOVERABLE_TRANSACTION_STALE_CLAIMANT_EPOCH;
            case CHECKPOINT_MISMATCH: return ResponseCode.RECOVERABLE_TRANSACTION_CHECKPOINT_MISMATCH;
            case DECISION_CONFLICT: return ResponseCode.RECOVERABLE_TRANSACTION_DECISION_CONFLICT;
            case HANDLE_COLLISION: return ResponseCode.RECOVERABLE_TRANSACTION_HANDLE_COLLISION;
            case NOT_FOUND: return ResponseCode.RECOVERABLE_TRANSACTION_NOT_FOUND;
            case STORAGE_ERROR: return ResponseCode.RECOVERABLE_TRANSACTION_STORAGE_ERROR;
            default: return ResponseCode.RECOVERABLE_TRANSACTION_INVALID_REQUEST;
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
