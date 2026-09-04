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
package org.apache.rocketmq.common.transaction;

import java.io.Serializable;
import java.util.Objects;

/** Stable immutable identity of a prepared half message. */
public final class PreparedTransactionHandle implements Serializable {
    private static final long serialVersionUID = 7730328193889392100L;

    private final int protocolVersion;
    private final String clusterName;
    private final String brokerName;
    private final String brokerAddress;
    private final String topic;
    private final int queueId;
    private final String producerGroup;
    private final String transactionalIdPrefix;
    private final long ownerEpoch;
    private final long claimantEpoch;
    private final int ownerSlot;
    private final long checkpointId;
    private final long messageSequence;
    private final String handleId;
    private final String payloadDigest;
    private final String messageId;
    private final String transactionId;
    private final int halfMessageQueueId;
    private final long halfMessageQueueOffset;
    private final long halfMessageCommitLogOffset;

    public PreparedTransactionHandle(int protocolVersion, String clusterName, String brokerName,
        String brokerAddress, String topic, int queueId, String producerGroup, String transactionalIdPrefix,
        long ownerEpoch, long claimantEpoch, int ownerSlot, long checkpointId, long messageSequence,
        String handleId, String payloadDigest, String messageId, String transactionId,
        int halfMessageQueueId, long halfMessageQueueOffset, long halfMessageCommitLogOffset) {
        if (protocolVersion <= 0 || queueId < 0 || ownerEpoch <= 0 || claimantEpoch <= 0 || ownerSlot < 0
            || checkpointId < 0 || messageSequence < 0 || halfMessageQueueId < 0
            || halfMessageQueueOffset < 0 || halfMessageCommitLogOffset < 0) {
            throw new IllegalArgumentException("invalid prepared transaction handle");
        }
        this.protocolVersion = protocolVersion;
        this.clusterName = requireText(clusterName, "clusterName");
        this.brokerName = requireText(brokerName, "brokerName");
        this.brokerAddress = requireText(brokerAddress, "brokerAddress");
        this.topic = requireText(topic, "topic");
        this.queueId = queueId;
        this.producerGroup = requireText(producerGroup, "producerGroup");
        this.transactionalIdPrefix = requireText(transactionalIdPrefix, "transactionalIdPrefix");
        this.ownerEpoch = ownerEpoch;
        this.claimantEpoch = claimantEpoch;
        this.ownerSlot = ownerSlot;
        this.checkpointId = checkpointId;
        this.messageSequence = messageSequence;
        this.handleId = requireText(handleId, "handleId");
        this.payloadDigest = requireText(payloadDigest, "payloadDigest");
        this.messageId = requireText(messageId, "messageId");
        this.transactionId = requireText(transactionId, "transactionId");
        this.halfMessageQueueId = halfMessageQueueId;
        this.halfMessageQueueOffset = halfMessageQueueOffset;
        this.halfMessageCommitLogOffset = halfMessageCommitLogOffset;
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isEmpty()) throw new IllegalArgumentException(name + " must not be empty");
        return value;
    }

    public int getProtocolVersion() { return protocolVersion; }
    public String getClusterName() { return clusterName; }
    public String getBrokerName() { return brokerName; }
    public String getBrokerAddress() { return brokerAddress; }
    public String getTopic() { return topic; }
    public int getQueueId() { return queueId; }
    public String getProducerGroup() { return producerGroup; }
    public String getTransactionalIdPrefix() { return transactionalIdPrefix; }
    public long getOwnerEpoch() { return ownerEpoch; }
    public long getClaimantEpoch() { return claimantEpoch; }
    public int getOwnerSlot() { return ownerSlot; }
    public long getCheckpointId() { return checkpointId; }
    public long getMessageSequence() { return messageSequence; }
    public String getHandleId() { return handleId; }
    public String getPayloadDigest() { return payloadDigest; }
    public String getMessageId() { return messageId; }
    public String getTransactionId() { return transactionId; }
    public int getHalfMessageQueueId() { return halfMessageQueueId; }
    public long getHalfMessageQueueOffset() { return halfMessageQueueOffset; }
    public long getHalfMessageCommitLogOffset() { return halfMessageCommitLogOffset; }

    public byte[] serialize() {
        return PreparedTransactionHandleSerializer.serialize(this);
    }

    public static PreparedTransactionHandle deserialize(byte[] bytes) {
        return PreparedTransactionHandleSerializer.deserialize(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PreparedTransactionHandle)) return false;
        PreparedTransactionHandle that = (PreparedTransactionHandle) o;
        return protocolVersion == that.protocolVersion && queueId == that.queueId
            && ownerEpoch == that.ownerEpoch && claimantEpoch == that.claimantEpoch
            && ownerSlot == that.ownerSlot && checkpointId == that.checkpointId
            && messageSequence == that.messageSequence && halfMessageQueueId == that.halfMessageQueueId
            && halfMessageQueueOffset == that.halfMessageQueueOffset
            && halfMessageCommitLogOffset == that.halfMessageCommitLogOffset
            && clusterName.equals(that.clusterName) && brokerName.equals(that.brokerName)
            && brokerAddress.equals(that.brokerAddress) && topic.equals(that.topic)
            && producerGroup.equals(that.producerGroup)
            && transactionalIdPrefix.equals(that.transactionalIdPrefix) && handleId.equals(that.handleId)
            && payloadDigest.equals(that.payloadDigest) && messageId.equals(that.messageId)
            && transactionId.equals(that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(protocolVersion, clusterName, brokerName, brokerAddress, topic, queueId,
            producerGroup, transactionalIdPrefix, ownerEpoch, claimantEpoch, ownerSlot, checkpointId,
            messageSequence, handleId, payloadDigest, messageId, transactionId, halfMessageQueueId,
            halfMessageQueueOffset, halfMessageCommitLogOffset);
    }
}
