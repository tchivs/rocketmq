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
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/** Versioned internal response body for the recoverable transaction RPC. */
public class RecoverableTransactionResponseBody extends RemotingSerializable {
    private int protocolVersion;
    private String clusterName;
    private String brokerName;
    private String brokerAddress;
    private String producerGroup;
    private String transactionalIdPrefix;
    private String claimantIdentity;
    private int ownerSlot;
    private long ownerEpoch;
    private long claimantEpoch;
    private long restoredCheckpointId;
    private long checkpointId;
    private String decision;
    private String outcome;
    private int preparedCount;
    private int completedCount;
    private boolean idempotent;
    private List<RecoverableTransactionHandleData> handles;
    private List<String> handleOutcomes;

    public int getProtocolVersion() { return protocolVersion; }
    public void setProtocolVersion(int protocolVersion) { this.protocolVersion = protocolVersion; }
    public String getClusterName() { return clusterName; }
    public void setClusterName(String clusterName) { this.clusterName = clusterName; }
    public String getBrokerName() { return brokerName; }
    public void setBrokerName(String brokerName) { this.brokerName = brokerName; }
    public String getBrokerAddress() { return brokerAddress; }
    public void setBrokerAddress(String brokerAddress) { this.brokerAddress = brokerAddress; }
    public String getProducerGroup() { return producerGroup; }
    public void setProducerGroup(String producerGroup) { this.producerGroup = producerGroup; }
    public String getTransactionalIdPrefix() { return transactionalIdPrefix; }
    public void setTransactionalIdPrefix(String transactionalIdPrefix) { this.transactionalIdPrefix = transactionalIdPrefix; }
    public String getClaimantIdentity() { return claimantIdentity; }
    public void setClaimantIdentity(String claimantIdentity) { this.claimantIdentity = claimantIdentity; }
    public int getOwnerSlot() { return ownerSlot; }
    public void setOwnerSlot(int ownerSlot) { this.ownerSlot = ownerSlot; }
    public long getOwnerEpoch() { return ownerEpoch; }
    public void setOwnerEpoch(long ownerEpoch) { this.ownerEpoch = ownerEpoch; }
    public long getClaimantEpoch() { return claimantEpoch; }
    public void setClaimantEpoch(long claimantEpoch) { this.claimantEpoch = claimantEpoch; }
    public long getRestoredCheckpointId() { return restoredCheckpointId; }
    public void setRestoredCheckpointId(long restoredCheckpointId) { this.restoredCheckpointId = restoredCheckpointId; }
    public long getCheckpointId() { return checkpointId; }
    public void setCheckpointId(long checkpointId) { this.checkpointId = checkpointId; }
    public String getDecision() { return decision; }
    public void setDecision(String decision) { this.decision = decision; }
    public String getOutcome() { return outcome; }
    public void setOutcome(String outcome) { this.outcome = outcome; }
    public int getPreparedCount() { return preparedCount; }
    public void setPreparedCount(int preparedCount) { this.preparedCount = preparedCount; }
    public int getCompletedCount() { return completedCount; }
    public void setCompletedCount(int completedCount) { this.completedCount = completedCount; }
    public boolean isIdempotent() { return idempotent; }
    public void setIdempotent(boolean idempotent) { this.idempotent = idempotent; }
    public List<RecoverableTransactionHandleData> getHandles() { return handles; }
    public void setHandles(List<RecoverableTransactionHandleData> handles) { this.handles = handles; }
    public List<String> getHandleOutcomes() { return handleOutcomes; }
    public void setHandleOutcomes(List<String> handleOutcomes) { this.handleOutcomes = handleOutcomes; }
}
