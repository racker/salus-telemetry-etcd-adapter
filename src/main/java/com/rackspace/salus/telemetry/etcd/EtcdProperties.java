/*
 * Copyright 2020 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.salus.telemetry.etcd;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties("salus.etcd")
@Component
@Data
public class EtcdProperties {
    String url = "http://localhost:2379";
    /**
     * If set, specifies the path to a CA PEM file to use for secured etcd connectivity.
     */
    String caCert;

    /**
     * If set, refers to a PEM file that contains the key material to enable mutual authentication
     * with the etcd server.
     */
    String key;

    /**
     * If <code>key</code> is set, this can optionally be set to the path of a PEM file that contains
     * the client certificate chain.
     */
    String keyCertChain;

    /**
     * If <code>key</code> is set, this can optionally be set if the key PEM is encrypted by a
     * password.
     */
    String keyPassword;

    long envoyLeaseSec = 30;

    /**
     * Configures the maximum size of the thread pool used for etcd client operations.
     */
    int maxExecutorThreads = 4;
    int executorOfferTimeoutSec = 30;
}
