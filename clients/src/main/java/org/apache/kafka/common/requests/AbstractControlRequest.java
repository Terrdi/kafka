/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;

// Abstract class for all control requests including UpdateMetadataRequest, LeaderAndIsrRequest and StopReplicaRequest
/**
 * LeaderAndLsrRequest: 告诉 Broker 相关主题各个分区的Leader副本位于哪台 Broker 上、ISR中的副本都在哪些Broker上。
 * 它应该被赋予最高的优先级，毕竟，它有令数据类请求直接失效的本领。
 *
 * StopReplicaRequest: 告知指定 Broker 停止它上面的副本对象，该请求甚至还能删除副本底层的日志数据。
 * 这个请求主要的使用场景，是分区副本迁移和删除主题。 这两个场景都涉及停掉 Broker 上的副本操作。
 *
 * UpdateMetadataRequest: 该请求会更新 Broker 上的元数据缓存。集群上的所有元数据变更，都首先发生在 Controller 端，然后
 * 再经由这个请求广播给集群上的所有 Broker。
 */
public abstract class AbstractControlRequest extends AbstractRequest {

    public static final long UNKNOWN_BROKER_EPOCH = -1L;

    public static abstract class Builder<T extends AbstractRequest> extends AbstractRequest.Builder<T> {
        protected final int controllerId;
        protected final int controllerEpoch;
        protected final long brokerEpoch;

        protected Builder(ApiKeys api, short version, int controllerId, int controllerEpoch, long brokerEpoch) {
            super(api, version);
            this.controllerId = controllerId;
            this.controllerEpoch = controllerEpoch;
            this.brokerEpoch = brokerEpoch;
        }

    }

    protected AbstractControlRequest(ApiKeys api, short version) {
        super(api, version);
    }

    /**
     * @return Controller 所在的 Broker ID
     */
    public abstract int controllerId();

    /**
     * @return Controller 的版本信息
     */
    public abstract int controllerEpoch();

    /**
     * @return 目标 Broker 的 Epoch
     */
    public abstract long brokerEpoch();

}
