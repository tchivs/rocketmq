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
import java.util.Arrays;
import java.util.Objects;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.ClaimMode;

/** Immutable request for fresh ownership, stateful resume, or explicit takeover. */
public final class RecoverableTransactionOwnerClaim implements Serializable {
    private static final long serialVersionUID = -759965509457392367L;

    private final int protocolVersion;
    private final String transactionalIdPrefix;
    private final String jobId;
    private final int ownerSlot;
    private final long attemptNumber;
    private final long restoredCheckpointId;
    private final long expectedOwnerEpoch;
    private final ClaimMode mode;

    public RecoverableTransactionOwnerClaim(String transactionalIdPrefix, String jobId, int ownerSlot,
        long attemptNumber, long restoredCheckpointId, long expectedOwnerEpoch, ClaimMode mode) {
        this(RecoverableTransactionProtocol.CURRENT_VERSION, transactionalIdPrefix, jobId, ownerSlot,
            attemptNumber, restoredCheckpointId, expectedOwnerEpoch, mode);
    }

    public RecoverableTransactionOwnerClaim(int protocolVersion, String transactionalIdPrefix, String jobId,
        int ownerSlot, long attemptNumber, long restoredCheckpointId, long expectedOwnerEpoch, ClaimMode mode) {
        if (protocolVersion <= 0 || ownerSlot < 0 || attemptNumber < 0 || restoredCheckpointId < -1
            || expectedOwnerEpoch < -1) {
            throw new IllegalArgumentException("invalid recoverable transaction owner claim");
        }
        this.protocolVersion = protocolVersion;
        this.transactionalIdPrefix = requireText(transactionalIdPrefix, "transactionalIdPrefix");
        this.jobId = requireText(jobId, "jobId");
        this.ownerSlot = ownerSlot;
        this.attemptNumber = attemptNumber;
        this.restoredCheckpointId = restoredCheckpointId;
        this.expectedOwnerEpoch = expectedOwnerEpoch;
        this.mode = Objects.requireNonNull(mode, "mode");
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public String getTransactionalIdPrefix() {
        return transactionalIdPrefix;
    }

    public String getJobId() {
        return jobId;
    }

    public int getOwnerSlot() {
        return ownerSlot;
    }

    public long getAttemptNumber() {
        return attemptNumber;
    }

    public long getRestoredCheckpointId() {
        return restoredCheckpointId;
    }

    public long getExpectedOwnerEpoch() {
        return expectedOwnerEpoch;
    }

    public ClaimMode getMode() {
        return mode;
    }

    public String claimantIdentity() {
        return RecoverableTransactionCheckpoint.computeDigest(Arrays.asList(
            "jobId:" + jobId, "ownerSlot:" + ownerSlot, "attemptNumber:" + attemptNumber,
            "restoredCheckpointId:" + restoredCheckpointId));
    }
}
