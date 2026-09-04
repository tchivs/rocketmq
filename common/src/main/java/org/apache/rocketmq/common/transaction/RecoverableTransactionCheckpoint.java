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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable per-broker checkpoint descriptor with a deterministic prepared-handle digest. */
public final class RecoverableTransactionCheckpoint implements Serializable {
    private static final long serialVersionUID = 283336051039415516L;

    private final int protocolVersion;
    private final String brokerName;
    private final String brokerAddress;
    private final String producerGroup;
    private final String transactionalIdPrefix;
    private final long ownerEpoch;
    private final long claimantEpoch;
    private final int ownerSlot;
    private final long checkpointId;
    private final List<PreparedTransactionHandle> handles;
    private final String digest;

    public RecoverableTransactionCheckpoint(Collection<PreparedTransactionHandle> handles) {
        if (handles == null || handles.isEmpty()) {
            throw new IllegalArgumentException("checkpoint must contain at least one prepared handle");
        }
        List<PreparedTransactionHandle> copy = new ArrayList<>(handles);
        copy.sort(Comparator.comparingLong(PreparedTransactionHandle::getMessageSequence));
        PreparedTransactionHandle first = copy.get(0);
        Set<String> ids = new HashSet<>();
        for (PreparedTransactionHandle handle : copy) {
            if (handle.getProtocolVersion() != first.getProtocolVersion()
                || !handle.getBrokerName().equals(first.getBrokerName())
                || !handle.getBrokerAddress().equals(first.getBrokerAddress())
                || !handle.getProducerGroup().equals(first.getProducerGroup())
                || !handle.getTransactionalIdPrefix().equals(first.getTransactionalIdPrefix())
                || handle.getOwnerEpoch() != first.getOwnerEpoch()
                || handle.getClaimantEpoch() != first.getClaimantEpoch()
                || handle.getOwnerSlot() != first.getOwnerSlot()
                || handle.getCheckpointId() != first.getCheckpointId()) {
                throw new IllegalArgumentException("all checkpoint handles must share broker and checkpoint identity");
            }
            if (!ids.add(handle.getHandleId())) {
                throw new IllegalArgumentException("duplicate prepared handle " + handle.getHandleId());
            }
        }
        this.protocolVersion = first.getProtocolVersion();
        this.brokerName = first.getBrokerName();
        this.brokerAddress = first.getBrokerAddress();
        this.producerGroup = first.getProducerGroup();
        this.transactionalIdPrefix = first.getTransactionalIdPrefix();
        this.ownerEpoch = first.getOwnerEpoch();
        this.ownerSlot = first.getOwnerSlot();
        this.claimantEpoch = first.getClaimantEpoch();
        this.checkpointId = first.getCheckpointId();
        this.handles = Collections.unmodifiableList(copy);
        this.digest = computeDigest(ids);
    }

    public static String computeDigest(Collection<String> handleIds) {
        Objects.requireNonNull(handleIds, "handleIds");
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            List<String> sorted = new ArrayList<>(handleIds);
            Collections.sort(sorted);
            ByteBuffer length = ByteBuffer.allocate(4);
            for (String id : sorted) {
                byte[] bytes = Objects.requireNonNull(id, "handleId").getBytes(StandardCharsets.UTF_8);
                length.clear();
                length.putInt(bytes.length);
                digest.update(length.array());
                digest.update(bytes);
            }
            return toHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
    public static String computeHandleId(String transactionalIdPrefix, long ownerEpoch,
        long claimantEpoch, int ownerSlot, long checkpointId, long messageSequence,
        String payloadDigest) {
        Objects.requireNonNull(transactionalIdPrefix, "transactionalIdPrefix");
        Objects.requireNonNull(payloadDigest, "payloadDigest");
        if (transactionalIdPrefix.isEmpty() || transactionalIdPrefix.indexOf('\u0001') >= 0
            || ownerEpoch <= 0 || claimantEpoch <= 0 || ownerSlot < 0 || checkpointId < 0
            || messageSequence < 0 || payloadDigest.isEmpty()) {
            throw new IllegalArgumentException("invalid prepared transaction identity");
        }
        String identity = transactionalIdPrefix + '\u0001' + ownerEpoch + '\u0001' + claimantEpoch
            + '\u0001' + ownerSlot + '\u0001' + checkpointId + '\u0001' + messageSequence
            + '\u0001' + payloadDigest;
        return computeDigest(Collections.singletonList(identity));
    }

    public static String sha256(byte[] value) {
        Objects.requireNonNull(value, "value");
        try {
            return toHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String toHex(byte[] bytes) {
        char[] chars = new char[bytes.length * 2];
        char[] alphabet = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            int value = bytes[i] & 0xff;
            chars[i * 2] = alphabet[value >>> 4];
            chars[i * 2 + 1] = alphabet[value & 0xf];
        }
        return new String(chars);
    }

    public int getProtocolVersion() { return protocolVersion; }
    public String getBrokerName() { return brokerName; }
    public String getBrokerAddress() { return brokerAddress; }
    public String getProducerGroup() { return producerGroup; }
    public String getTransactionalIdPrefix() { return transactionalIdPrefix; }
    public long getOwnerEpoch() { return ownerEpoch; }
    public long getClaimantEpoch() { return claimantEpoch; }
    public int getOwnerSlot() { return ownerSlot; }
    public long getCheckpointId() { return checkpointId; }
    public int getPreparedCount() { return handles.size(); }
    public List<PreparedTransactionHandle> getHandles() { return handles; }
    public String getDigest() { return digest; }
}
