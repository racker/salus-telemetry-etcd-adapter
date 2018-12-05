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

import static com.rackspace.salus.telemetry.etcd.EtcdUtils.completedPutResponse;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.net.SocketAddress;

import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.model.NodeInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@Import(KeyHashing.class)
@SpringBootTest(webEnvironment = WebEnvironment.NONE)
@JsonTest // sets up ObjectMapper
public class EnvoyNodeManagementTest {

    @MockBean
    Client etcd;

    @Mock
    KV kv;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KeyHashing hashing;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    EnvoyNodeManagement envoyNodeManagement;


    @Before
    public void setUp() {
        when(etcd.getKVClient()).thenReturn(kv);
    }

    @Test
    public void testRegister() {
        Map<String, String> envoyLabels = new HashMap<>();
        envoyLabels.put("os", "LINUX");
        envoyLabels.put("arch", "X86_64");

        String envoyId = "abcde";
        String tenantId = "123456";
        String identifier = "os";
        String identifierValue = envoyLabels.get(identifier);
        long leaseId = 50;
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

        when(kv.put(argThat(t -> t.toStringUtf8().startsWith("/nodes/expected")), any()))
                .thenReturn(completedPutResponse());
        when(kv.put(argThat(t -> t.toStringUtf8().startsWith("/nodes/active")), any(), any()))
                .thenReturn(completedPutResponse());

        envoyNodeManagement.registerNode(tenantId, envoyId, leaseId, identifier, envoyLabels, address).join();

        verifyPut("/nodes/active/" + nodeKeyHash, nodeInfo, leaseId);
        verifyPut("/nodes/expected/" + nodeKeyHash, nodeInfo, null);
    }

    private void verifyPut(String k, NodeInfo v, Long leaseId) {
        ByteSequence valueBytes;
        try {
            valueBytes = ByteSequence.fromBytes(objectMapper.writeValueAsBytes(v));
        } catch (JsonProcessingException e) {
            assertNull(e);
            valueBytes = null;
        }

        if (leaseId != null) {
            verify(kv).put(
                    eq(ByteSequence.fromString(k)),
                    eq(valueBytes),
                    argThat(putOption -> putOption.getLeaseId() == leaseId));
        } else {
            verify(kv).put(
                    eq(ByteSequence.fromString(k)),
                    eq(valueBytes));
        }
    }
}
