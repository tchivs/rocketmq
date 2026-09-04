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

/** Immutable capability returned by a broker that supports recoverable transactions. */
public final class RecoverableTransactionCapability implements Serializable {
    private static final long serialVersionUID = 3078724986317189758L;

    private final int protocolVersion;
    private final String clusterName;
    private final String brokerName;
    private final String brokerAddress;

    public RecoverableTransactionCapability(int protocolVersion, String clusterName, String brokerName,
        String brokerAddress) {
        if (protocolVersion <= 0) {
            throw new IllegalArgumentException("protocolVersion must be positive");
        }
        this.protocolVersion = protocolVersion;
        this.clusterName = requireText(clusterName, "clusterName");
        this.brokerName = requireText(brokerName, "brokerName");
        this.brokerAddress = requireText(brokerAddress, "brokerAddress");
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

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecoverableTransactionCapability)) {
            return false;
        }
        RecoverableTransactionCapability that = (RecoverableTransactionCapability) o;
        return protocolVersion == that.protocolVersion && clusterName.equals(that.clusterName)
            && brokerName.equals(that.brokerName) && brokerAddress.equals(that.brokerAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(protocolVersion, clusterName, brokerName, brokerAddress);
    }
}
