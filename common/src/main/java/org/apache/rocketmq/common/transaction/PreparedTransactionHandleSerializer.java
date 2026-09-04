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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Explicit versioned binary serializer for {@link PreparedTransactionHandle}. */
public final class PreparedTransactionHandleSerializer {
    private static final int MAGIC = 0x52545848;
    private static final int MAX_STRING_BYTES = 1024 * 1024;

    private PreparedTransactionHandleSerializer() {
    }

    public static byte[] serialize(PreparedTransactionHandle handle) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(384);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeInt(handle.getProtocolVersion());
            writeString(out, handle.getClusterName());
            writeString(out, handle.getBrokerName());
            writeString(out, handle.getBrokerAddress());
            writeString(out, handle.getTopic());
            out.writeInt(handle.getQueueId());
            writeString(out, handle.getProducerGroup());
            writeString(out, handle.getTransactionalIdPrefix());
            out.writeLong(handle.getOwnerEpoch());
            out.writeLong(handle.getClaimantEpoch());
            out.writeInt(handle.getOwnerSlot());
            out.writeLong(handle.getCheckpointId());
            out.writeLong(handle.getMessageSequence());
            writeString(out, handle.getHandleId());
            writeString(out, handle.getPayloadDigest());
            writeString(out, handle.getMessageId());
            writeString(out, handle.getTransactionId());
            out.writeInt(handle.getHalfMessageQueueId());
            out.writeLong(handle.getHalfMessageQueueOffset());
            out.writeLong(handle.getHalfMessageCommitLogOffset());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize prepared transaction handle", e);
        }
    }

    public static PreparedTransactionHandle deserialize(byte[] serialized) {
        if (serialized == null) throw new IllegalArgumentException("serialized handle must not be null");
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
            if (in.readInt() != MAGIC) throw new IllegalArgumentException("invalid prepared handle magic");
            int version = in.readInt();
            if (version != RecoverableTransactionProtocol.VERSION_1) {
                throw new IllegalArgumentException("unsupported prepared handle version " + version);
            }
            PreparedTransactionHandle handle = new PreparedTransactionHandle(version, readString(in),
                readString(in), readString(in), readString(in), in.readInt(), readString(in), readString(in),
                in.readLong(), in.readLong(), in.readInt(), in.readLong(), in.readLong(), readString(in),
                readString(in), readString(in), readString(in), in.readInt(), in.readLong(), in.readLong());
            if (in.available() != 0) throw new IllegalArgumentException("trailing prepared handle data");
            return handle;
        } catch (EOFException e) {
            throw new IllegalArgumentException("truncated prepared transaction handle", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid prepared transaction handle", e);
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        if (data.length > MAX_STRING_BYTES) throw new IllegalArgumentException("handle field is too large");
        out.writeInt(data.length);
        out.write(data);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_STRING_BYTES || length > in.available()) {
            throw new IllegalArgumentException("invalid handle string length " + length);
        }
        byte[] data = new byte[length];
        in.readFully(data);
        return new String(data, StandardCharsets.UTF_8);
    }
}
