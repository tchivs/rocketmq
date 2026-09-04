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

/** Public constants and enums for the recoverable transaction protocol. */
public final class RecoverableTransactionProtocol {
    public static final int VERSION_1 = 1;
    public static final int CURRENT_VERSION = VERSION_1;

    private RecoverableTransactionProtocol() {
    }

    public enum Operation {
        CAPABILITY,
        CLAIM,
        PREPARE,
        FINALIZE,
        QUERY,
        LIST,
        ABORT
    }

    public enum ClaimMode {
        FRESH,
        RESUME,
        TAKEOVER
    }

    public enum Decision {
        COMMIT,
        ROLLBACK
    }

    public enum Outcome {
        PENDING,
        COMMITTING,
        COMMITTED,
        ROLLED_BACK
    }

    public enum ErrorCode {
        UNSUPPORTED_VERSION,
        INVALID_REQUEST,
        OWNERSHIP_CONFLICT,
        STALE_OWNER_EPOCH,
        STALE_CLAIMANT_EPOCH,
        CHECKPOINT_MISMATCH,
        DECISION_CONFLICT,
        HANDLE_COLLISION,
        NOT_FOUND,
        STORAGE_ERROR
    }
}
