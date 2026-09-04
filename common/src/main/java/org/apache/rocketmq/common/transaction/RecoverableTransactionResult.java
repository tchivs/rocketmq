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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.Decision;
import org.apache.rocketmq.common.transaction.RecoverableTransactionProtocol.Outcome;

/** Immutable synchronous finalize/query result. */
public final class RecoverableTransactionResult implements Serializable {
    private static final long serialVersionUID = 6775862941250667202L;

    private final String transactionalIdPrefix;
    private final long ownerEpoch;
    private final int ownerSlot;
    private final long checkpointId;
    private final Decision decision;
    private final Outcome outcome;
    private final int preparedCount;
    private final int completedCount;
    private final boolean idempotent;
    private final List<PreparedTransactionHandle> pendingHandles;

    public RecoverableTransactionResult(String transactionalIdPrefix, long ownerEpoch, int ownerSlot,
        long checkpointId, Decision decision, Outcome outcome, int preparedCount, int completedCount,
        boolean idempotent, List<PreparedTransactionHandle> pendingHandles) {
        this.transactionalIdPrefix = Objects.requireNonNull(transactionalIdPrefix, "transactionalIdPrefix");
        if (transactionalIdPrefix.isEmpty() || ownerEpoch <= 0 || ownerSlot < 0 || checkpointId < 0
            || preparedCount < 0 || completedCount < 0 || completedCount > preparedCount) {
            throw new IllegalArgumentException("invalid recoverable transaction result");
        }
        this.ownerEpoch = ownerEpoch;
        this.ownerSlot = ownerSlot;
        this.checkpointId = checkpointId;
        this.decision = decision;
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.preparedCount = preparedCount;
        this.completedCount = completedCount;
        this.idempotent = idempotent;
        this.pendingHandles = Collections.unmodifiableList(new ArrayList<>(
            Objects.requireNonNull(pendingHandles, "pendingHandles")));
    }

    public String getTransactionalIdPrefix() { return transactionalIdPrefix; }
    public long getOwnerEpoch() { return ownerEpoch; }
    public int getOwnerSlot() { return ownerSlot; }
    public long getCheckpointId() { return checkpointId; }
    public Decision getDecision() { return decision; }
    public Outcome getOutcome() { return outcome; }
    public int getPreparedCount() { return preparedCount; }
    public int getCompletedCount() { return completedCount; }
    public boolean isIdempotent() { return idempotent; }
    public List<PreparedTransactionHandle> getPendingHandles() { return pendingHandles; }
}
