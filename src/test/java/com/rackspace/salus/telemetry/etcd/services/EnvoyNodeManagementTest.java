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
import static org.junit.Assert.*;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.lease.LeaseGrantResponse;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.model.NodeConnectionStatus;
import com.rackspace.salus.telemetry.model.NodeInfo;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.NONE)
@JsonTest // sets up ObjectMapper
public class EnvoyNodeManagementTest {

    @Configuration
    @Import({KeyHashing.class, EnvoyNodeManagement.class})
    public static class TestConfig {
        @Bean
        public Client getClient() {
            final List<String> endpoints = etcd.cluster().getClientEndpoints().stream()
                    .map(URI::toString)
                    .collect(Collectors.toList());
            return Client.builder().endpoints(endpoints).build();
        }
    }

    @ClassRule
    public static final EtcdClusterResource etcd = new EtcdClusterResource("test-etcd", 1);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KeyHashing hashing;

    @Autowired
    private Client client;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    EnvoyNodeManagement envoyNodeManagement;

    @Test
    public void testRegisterAndRemove() {
        Date startedDate = new Date();
        Map<String, String> envoyLabels = new HashMap<>();
        envoyLabels.put("os", "LINUX");
        envoyLabels.put("arch", "X86_64");

        String envoyId = "abcde";
        String tenantId = "123456";
        String identifier = "os";
        String identifierValue = envoyLabels.get(identifier);
        long leaseId = 0;
        try {
            leaseId = client.getLeaseClient().grant(10000)
                    .thenApply(LeaseGrantResponse::getID).get();
        } catch (ExecutionException | InterruptedException e) {
            assertNull(e);
        }
        InetSocketAddress address;
        try {
            address = new InetSocketAddress(InetAddress.getLocalHost(), 1234);
        } catch (UnknownHostException e) {
            assertNull(e);
            address = null;
        }

        String nodeKey = String.format("%s:%s:%s", tenantId, identifier, identifierValue);
        final String nodeKeyHash = hashing.hash(nodeKey);

        NodeInfo nodeInfo = new NodeInfo()
                .setEnvoyId(envoyId)
                .setIdentifier(identifier)
                .setIdentifierValue(identifierValue)
                .setLabels(envoyLabels)
                .setTenantId(tenantId)
                .setAddress(address);

        String identifierPath = String.format("/tenants/%s/identifiers/%s:%s",
                tenantId, identifier, identifierValue);

        envoyNodeManagement.registerNode(tenantId, envoyId, leaseId, identifier, envoyLabels, address).join();

        verifyNodeInfo("/nodes/active/" + nodeKeyHash, nodeInfo, leaseId);
        verifyNodeInfo("/nodes/expected/" + nodeKeyHash, nodeInfo, null);
        verifyNodeConnectionStatus(identifierPath, startedDate);

        envoyNodeManagement.removeNode(tenantId, identifier, identifierValue).join();

        verifyDelete("/nodes/active/" + nodeKeyHash);
        verifyDelete("/nodes/expected/" + nodeKeyHash);
        verifyDelete(identifierPath);
    }

    private void verifyNodeInfo(String k, NodeInfo v, Long leaseId) {
        client.getKVClient().get(buildKey(k))
                .thenApply(getResponse -> {
                    assertEquals("Only stored 1 item for key so should only receive 1",
                            1, getResponse.getCount());
                    KeyValue storedData = getResponse.getKvs().get(0);

                    String key = storedData.getKey().toStringUtf8();
                    NodeInfo nodeInfo;
                    try {;
                        nodeInfo = objectMapper.readValue(storedData.getValue().getBytes(), NodeInfo.class);
                    } catch (IOException e) {
                        assertNull("Any exception should cause a failure", e);
                        return false;
                    }
                    assertEquals(k, key);
                    assertEquals(v.getEnvoyId(), nodeInfo.getEnvoyId());
                    assertEquals(v.getIdentifier(), nodeInfo.getIdentifier());
                    assertEquals(v.getIdentifierValue(), nodeInfo.getIdentifierValue());
                    assertEquals(v.getTenantId(), nodeInfo.getTenantId());
                    assertEquals(v.getAddress().getHostName(), nodeInfo.getAddress().getHostName());
                    assertEquals(v.getAddress().getPort(), nodeInfo.getAddress().getPort());
                    assertEquals(v.getLabels().size(), nodeInfo.getLabels().size());

                    if (leaseId != null) {
                        assertEquals(leaseId.longValue(), storedData.getLease());
                    }
                    return true;
                }).join();
    }

    private void verifyNodeConnectionStatus(String k, Date startedDate) {
        client.getKVClient().get(buildKey(k))
                .thenApply(getResponse -> {
                    assertEquals("Only stored 1 item for key so should only receive 1",
                            1, getResponse.getCount());
                    KeyValue storedData = getResponse.getKvs().get(0);

                    String key = storedData.getKey().toStringUtf8();
                    NodeConnectionStatus connectionStatus;
                    try {
                        connectionStatus = objectMapper.readValue(storedData.getValue().getBytes(),
                                NodeConnectionStatus.class);
                    } catch (IOException e) {
                        assertNull("Any exception should cause a failure", e);
                        return false;
                    }
                    assertEquals(k, key);
                    assertTrue(connectionStatus.isConnected());
                    assertTrue(connectionStatus.getLastConnectedTime().after(startedDate));

                    return true;
                }).join();
    }

    private void verifyDelete(String k){
        client.getKVClient().get(buildKey(k))
                .thenApply(getResponse -> {
                    assertEquals("No data should be found", 0, getResponse.getCount());
                    return true;
                }).join();
    }
}
