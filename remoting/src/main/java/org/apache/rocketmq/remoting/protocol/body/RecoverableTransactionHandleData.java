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
package org.apache.rocketmq.remoting.protocol.body;

import org.apache.rocketmq.common.transaction.PreparedTransactionHandle;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/** Mutable wire projection of the immutable public prepared handle. */
public class RecoverableTransactionHandleData extends RemotingSerializable {
    private int protocolVersion;
    private String clusterName;
    private String brokerName;
    private String brokerAddress;
    private String topic;
    private int queueId;
    private String producerGroup;
    private String transactionalIdPrefix;
    private long ownerEpoch;
    private long claimantEpoch;
    private int ownerSlot;
    private long checkpointId;
    private long messageSequence;
    private String handleId;
    private String payloadDigest;
    private String messageId;
    private String transactionId;
    private int halfMessageQueueId;
    private long halfMessageQueueOffset;
    private long halfMessageCommitLogOffset;

    public static RecoverableTransactionHandleData from(PreparedTransactionHandle handle) {
        RecoverableTransactionHandleData data = new RecoverableTransactionHandleData();
        data.protocolVersion = handle.getProtocolVersion();
        data.clusterName = handle.getClusterName();
        data.brokerName = handle.getBrokerName();
        data.brokerAddress = handle.getBrokerAddress();
        data.topic = handle.getTopic();
        data.queueId = handle.getQueueId();
        data.producerGroup = handle.getProducerGroup();
        data.transactionalIdPrefix = handle.getTransactionalIdPrefix();
        data.ownerEpoch = handle.getOwnerEpoch();
        data.claimantEpoch = handle.getClaimantEpoch();
        data.ownerSlot = handle.getOwnerSlot();
        data.checkpointId = handle.getCheckpointId();
        data.messageSequence = handle.getMessageSequence();
        data.handleId = handle.getHandleId();
        data.payloadDigest = handle.getPayloadDigest();
        data.messageId = handle.getMessageId();
        data.transactionId = handle.getTransactionId();
        data.halfMessageQueueId = handle.getHalfMessageQueueId();
        data.halfMessageQueueOffset = handle.getHalfMessageQueueOffset();
        data.halfMessageCommitLogOffset = handle.getHalfMessageCommitLogOffset();
        return data;
    }

    public PreparedTransactionHandle toHandle() {
        return new PreparedTransactionHandle(protocolVersion, clusterName, brokerName, brokerAddress, topic,
            queueId, producerGroup, transactionalIdPrefix, ownerEpoch, claimantEpoch, ownerSlot, checkpointId,
            messageSequence, handleId, payloadDigest, messageId, transactionId, halfMessageQueueId,
            halfMessageQueueOffset, halfMessageCommitLogOffset);
    }

    public int getProtocolVersion() { return protocolVersion; }
    public void setProtocolVersion(int protocolVersion) { this.protocolVersion = protocolVersion; }
    public String getClusterName() { return clusterName; }
    public void setClusterName(String clusterName) { this.clusterName = clusterName; }
    public String getBrokerName() { return brokerName; }
    public void setBrokerName(String brokerName) { this.brokerName = brokerName; }
    public String getBrokerAddress() { return brokerAddress; }
    public void setBrokerAddress(String brokerAddress) { this.brokerAddress = brokerAddress; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public int getQueueId() { return queueId; }
    public void setQueueId(int queueId) { this.queueId = queueId; }
    public String getProducerGroup() { return producerGroup; }
    public void setProducerGroup(String producerGroup) { this.producerGroup = producerGroup; }
    public String getTransactionalIdPrefix() { return transactionalIdPrefix; }
    public void setTransactionalIdPrefix(String transactionalIdPrefix) { this.transactionalIdPrefix = transactionalIdPrefix; }
    public long getOwnerEpoch() { return ownerEpoch; }
    public void setOwnerEpoch(long ownerEpoch) { this.ownerEpoch = ownerEpoch; }
    public long getClaimantEpoch() { return claimantEpoch; }
    public void setClaimantEpoch(long claimantEpoch) { this.claimantEpoch = claimantEpoch; }
    public int getOwnerSlot() { return ownerSlot; }
    public void setOwnerSlot(int ownerSlot) { this.ownerSlot = ownerSlot; }
    public long getCheckpointId() { return checkpointId; }
    public void setCheckpointId(long checkpointId) { this.checkpointId = checkpointId; }
    public long getMessageSequence() { return messageSequence; }
    public void setMessageSequence(long messageSequence) { this.messageSequence = messageSequence; }
    public String getHandleId() { return handleId; }
    public void setHandleId(String handleId) { this.handleId = handleId; }
    public String getPayloadDigest() { return payloadDigest; }
    public void setPayloadDigest(String payloadDigest) { this.payloadDigest = payloadDigest; }
    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public int getHalfMessageQueueId() { return halfMessageQueueId; }
    public void setHalfMessageQueueId(int halfMessageQueueId) { this.halfMessageQueueId = halfMessageQueueId; }
    public long getHalfMessageQueueOffset() { return halfMessageQueueOffset; }
    public void setHalfMessageQueueOffset(long halfMessageQueueOffset) { this.halfMessageQueueOffset = halfMessageQueueOffset; }
    public long getHalfMessageCommitLogOffset() { return halfMessageCommitLogOffset; }
    public void setHalfMessageCommitLogOffset(long halfMessageCommitLogOffset) { this.halfMessageCommitLogOffset = halfMessageCommitLogOffset; }
}
