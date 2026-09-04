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

import java.util.Arrays;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PreparedTransactionHandleTest {

    @Test
    public void roundTripsEveryStableFieldWithExplicitVersion() {
        PreparedTransactionHandle handle = handle(7, 2, 41);
        assertThat(PreparedTransactionHandle.deserialize(handle.serialize())).isEqualTo(handle);
        byte[] corrupt = handle.serialize();
        corrupt[0] = 0;
        assertThatThrownBy(() -> PreparedTransactionHandle.deserialize(corrupt))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void checkpointDigestIsOrderIndependentAndRejectsMixedIdentity() {
        PreparedTransactionHandle first = handle(7, 0, 40);
        PreparedTransactionHandle second = handle(7, 1, 41);
        RecoverableTransactionCheckpoint ordered = new RecoverableTransactionCheckpoint(
            Arrays.asList(first, second));
        RecoverableTransactionCheckpoint reversed = new RecoverableTransactionCheckpoint(
            Arrays.asList(second, first));
        assertThat(reversed.getDigest()).isEqualTo(ordered.getDigest());
        assertThat(reversed.getHandles()).containsExactly(first, second);
        assertThatThrownBy(() -> new RecoverableTransactionCheckpoint(
            Arrays.asList(first, handle(8, 2, 42))))
            .isInstanceOf(IllegalArgumentException.class);
    }

    private PreparedTransactionHandle handle(long checkpointId, long sequence, long offset) {
        return new PreparedTransactionHandle(RecoverableTransactionProtocol.CURRENT_VERSION,
            "cluster", "broker", "127.0.0.1:10911", "topic", 0, "group", "prefix", 3, 5,
            1, checkpointId, sequence, "handle-" + checkpointId + '-' + sequence,
            "payload-digest", "message-" + offset, "transaction-" + offset, 0, offset, offset * 100);
    }
}
