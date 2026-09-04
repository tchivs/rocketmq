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

/** Broker-issued fencing token for one owner slot on one broker. */
public final class RecoverableTransactionOwner implements Serializable {
    private static final long serialVersionUID = 4748658814987141579L;

    private final int protocolVersion;
    private final String clusterName;
    private final String brokerName;
    private final String brokerAddress;
    private final String producerGroup;
    private final String transactionalIdPrefix;
    private final String claimantIdentity;
    private final int ownerSlot;
    private final long ownerEpoch;
    private final long claimantEpoch;
    private final long restoredCheckpointId;

    public RecoverableTransactionOwner(int protocolVersion, String clusterName, String brokerName,
        String brokerAddress, String producerGroup, String transactionalIdPrefix, String claimantIdentity,
        int ownerSlot, long ownerEpoch, long claimantEpoch, long restoredCheckpointId) {
        if (protocolVersion <= 0 || ownerSlot < 0 || ownerEpoch <= 0 || claimantEpoch <= 0
            || restoredCheckpointId < -1) {
            throw new IllegalArgumentException("invalid recoverable transaction owner");
        }
        this.protocolVersion = protocolVersion;
        this.clusterName = requireText(clusterName, "clusterName");
        this.brokerName = requireText(brokerName, "brokerName");
        this.brokerAddress = requireText(brokerAddress, "brokerAddress");
        this.producerGroup = requireText(producerGroup, "producerGroup");
        this.transactionalIdPrefix = requireText(transactionalIdPrefix, "transactionalIdPrefix");
        this.claimantIdentity = requireText(claimantIdentity, "claimantIdentity");
        this.ownerSlot = ownerSlot;
        this.ownerEpoch = ownerEpoch;
        this.claimantEpoch = claimantEpoch;
        this.restoredCheckpointId = restoredCheckpointId;
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    public int getProtocolVersion() { return protocolVersion; }
    public String getClusterName() { return clusterName; }
    public String getBrokerName() { return brokerName; }
    public String getBrokerAddress() { return brokerAddress; }
    public String getProducerGroup() { return producerGroup; }
    public String getTransactionalIdPrefix() { return transactionalIdPrefix; }
    public String getClaimantIdentity() { return claimantIdentity; }
    public int getOwnerSlot() { return ownerSlot; }
    public long getOwnerEpoch() { return ownerEpoch; }
    public long getClaimantEpoch() { return claimantEpoch; }
    public long getRestoredCheckpointId() { return restoredCheckpointId; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecoverableTransactionOwner)) return false;
        RecoverableTransactionOwner that = (RecoverableTransactionOwner) o;
        return protocolVersion == that.protocolVersion && ownerSlot == that.ownerSlot
            && ownerEpoch == that.ownerEpoch && claimantEpoch == that.claimantEpoch
            && restoredCheckpointId == that.restoredCheckpointId && clusterName.equals(that.clusterName)
            && brokerName.equals(that.brokerName) && brokerAddress.equals(that.brokerAddress)
            && producerGroup.equals(that.producerGroup)
            && transactionalIdPrefix.equals(that.transactionalIdPrefix)
            && claimantIdentity.equals(that.claimantIdentity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(protocolVersion, clusterName, brokerName, brokerAddress, producerGroup,
            transactionalIdPrefix, claimantIdentity, ownerSlot, ownerEpoch, claimantEpoch,
            restoredCheckpointId);
    }
}
