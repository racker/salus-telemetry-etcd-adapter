/*
 *    Copyright 2018 Rackspace US, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 *
 */

package com.rackspace.salus.telemetry.etcd.services;

import static com.rackspace.salus.telemetry.etcd.EtcdUtils.buildKey;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.options.PutOption;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.model.NodeInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class EnvoyNodeManagement {

    private final Client etcd;
    private final ObjectMapper objectMapper;
    private final KeyHashing hashing;

    @Autowired
    public EnvoyNodeManagement(Client etcd, ObjectMapper objectMapper, KeyHashing hashing) {
        this.etcd = etcd;
        this.objectMapper = objectMapper;
        this.hashing = hashing;
    }

    /**
     * Creates the /active and /expected keys within etcd for the connected envoy.
     * /active is created using a lease, whereas /expected will live forever.
     *
     * @param tenantId The tenant used to authenticate the the envoy.
     * @param envoyId The auto-generated unique string associated to the envoy.
     * @param leaseId The lease used when creating the /active key.
     * @param identifier The key of the label used in envoy presence monitoring.
     * @param envoyLabels All labels associated with the envoy.
     * @param remoteAddr The address the envoy is connecting from.
     * @return The results of an etcd PUT.
     */
    public CompletableFuture<?> registerNode(String tenantId, String envoyId, long leaseId,
                                             String identifier, Map<String, String> envoyLabels,
                                             SocketAddress remoteAddr) {
        final PutOption putLeaseOption = PutOption.newBuilder()
                .withLeaseId(leaseId)
                .build();

        String identifierValue = envoyLabels.get(identifier);
        String nodeKey = String.format("%s:%s:%s", tenantId, identifier, identifierValue);
        NodeInfo nodeInfo = new NodeInfo()
                .setEnvoyId(envoyId)
                .setIdentifier(identifier)
                .setIdentifierValue(identifierValue)
                .setLabels(envoyLabels)
                .setTenantId(tenantId)
                .setAddress((InetSocketAddress) remoteAddr);

        final String nodeKeyHash = hashing.hash(nodeKey);
        final ByteSequence nodeInfoBytes;
        try {
            nodeInfoBytes = ByteSequence.fromBytes(objectMapper.writeValueAsBytes(nodeInfo));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to marshal NodeInfo", e);
        }

        return etcd.getKVClient().put(
                buildKey(Keys.FMT_NODES_EXPECTED, nodeKeyHash), nodeInfoBytes)
                .thenCompose(putResponse ->
                        etcd.getKVClient().put(
                                buildKey(Keys.FMT_NODES_ACTIVE, nodeKeyHash), nodeInfoBytes, putLeaseOption));
    }
}
