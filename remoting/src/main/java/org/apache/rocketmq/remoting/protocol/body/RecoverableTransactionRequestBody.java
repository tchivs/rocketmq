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

import java.util.List;
import java.util.Map;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/** Versioned internal request body for the recoverable transaction RPC. */
public class RecoverableTransactionRequestBody extends RemotingSerializable {
    private int protocolVersion;
    private String operation;
    private String producerGroup;
    private String transactionalIdPrefix;
    private String jobId;
    private int ownerSlot;
    private long attemptNumber;
    private long restoredCheckpointId;
    private long expectedOwnerEpoch;
    private String claimMode;
    private long ownerEpoch;
    private long claimantEpoch;
    private String claimantIdentity;
    private long checkpointId;
    private String decision;
    private int preparedCount;
    private String digest;
    private String handleId;
    private String payloadDigest;
    private long messageSequence;
    private String topic;
    private int queueId;
    private int flag;
    private long bornTimestamp;
    private byte[] messageBody;
    private Map<String, String> messageProperties;
    private List<RecoverableTransactionHandleData> handles;

    public int getProtocolVersion() { return protocolVersion; }
    public void setProtocolVersion(int protocolVersion) { this.protocolVersion = protocolVersion; }
    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }
    public String getProducerGroup() { return producerGroup; }
    public void setProducerGroup(String producerGroup) { this.producerGroup = producerGroup; }
    public String getTransactionalIdPrefix() { return transactionalIdPrefix; }
    public void setTransactionalIdPrefix(String transactionalIdPrefix) { this.transactionalIdPrefix = transactionalIdPrefix; }
    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
    public int getOwnerSlot() { return ownerSlot; }
    public void setOwnerSlot(int ownerSlot) { this.ownerSlot = ownerSlot; }
    public long getAttemptNumber() { return attemptNumber; }
    public void setAttemptNumber(long attemptNumber) { this.attemptNumber = attemptNumber; }
    public long getRestoredCheckpointId() { return restoredCheckpointId; }
    public void setRestoredCheckpointId(long restoredCheckpointId) { this.restoredCheckpointId = restoredCheckpointId; }
    public long getExpectedOwnerEpoch() { return expectedOwnerEpoch; }
    public void setExpectedOwnerEpoch(long expectedOwnerEpoch) { this.expectedOwnerEpoch = expectedOwnerEpoch; }
    public String getClaimMode() { return claimMode; }
    public void setClaimMode(String claimMode) { this.claimMode = claimMode; }
    public long getOwnerEpoch() { return ownerEpoch; }
    public void setOwnerEpoch(long ownerEpoch) { this.ownerEpoch = ownerEpoch; }
    public long getClaimantEpoch() { return claimantEpoch; }
    public void setClaimantEpoch(long claimantEpoch) { this.claimantEpoch = claimantEpoch; }
    public String getClaimantIdentity() { return claimantIdentity; }
    public void setClaimantIdentity(String claimantIdentity) { this.claimantIdentity = claimantIdentity; }
    public long getCheckpointId() { return checkpointId; }
    public void setCheckpointId(long checkpointId) { this.checkpointId = checkpointId; }
    public String getDecision() { return decision; }
    public void setDecision(String decision) { this.decision = decision; }
    public int getPreparedCount() { return preparedCount; }
    public void setPreparedCount(int preparedCount) { this.preparedCount = preparedCount; }
    public String getDigest() { return digest; }
    public void setDigest(String digest) { this.digest = digest; }
    public String getHandleId() { return handleId; }
    public void setHandleId(String handleId) { this.handleId = handleId; }
    public String getPayloadDigest() { return payloadDigest; }
    public void setPayloadDigest(String payloadDigest) { this.payloadDigest = payloadDigest; }
    public long getMessageSequence() { return messageSequence; }
    public void setMessageSequence(long messageSequence) { this.messageSequence = messageSequence; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public int getQueueId() { return queueId; }
    public void setQueueId(int queueId) { this.queueId = queueId; }
    public int getFlag() { return flag; }
    public void setFlag(int flag) { this.flag = flag; }
    public long getBornTimestamp() { return bornTimestamp; }
    public void setBornTimestamp(long bornTimestamp) { this.bornTimestamp = bornTimestamp; }
    public byte[] getMessageBody() { return messageBody; }
    public void setMessageBody(byte[] messageBody) { this.messageBody = messageBody; }
    public Map<String, String> getMessageProperties() { return messageProperties; }
    public void setMessageProperties(Map<String, String> messageProperties) { this.messageProperties = messageProperties; }
    public List<RecoverableTransactionHandleData> getHandles() { return handles; }
    public void setHandles(List<RecoverableTransactionHandleData> handles) { this.handles = handles; }
}
