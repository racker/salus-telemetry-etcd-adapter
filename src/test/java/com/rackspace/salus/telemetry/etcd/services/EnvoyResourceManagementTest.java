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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.util.KeyHashing;
import com.rackspace.salus.telemetry.model.ResourceInfo;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@JsonTest
public class EnvoyResourceManagementTest {

    @Configuration
    @Import({KeyHashing.class, EnvoyResourceManagement.class})
    public static class TestConfig {
        @Bean
        public Client getClient() {
            return Client.builder().endpoints(
                etcd.cluster().getClientEndpoints()
            ).build();
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
    EnvoyResourceManagement envoyResourceManagement;

    @Test
    public void testRegisterAndRemove() {
        Map<String, String> envoyLabels = new HashMap<>();
        envoyLabels.put("os", "LINUX");
        envoyLabels.put("arch", "X86_64");

        String envoyId = "abcde";
        String tenantId = "123456";
        String resourceId = "hostname:localhost";
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

        String resourceKey = String.format("%s:%s", tenantId, resourceId);
        final String resourceKeyHash = hashing.hash(resourceKey);

        ResourceInfo resourceInfo = new ResourceInfo()
                .setEnvoyId(envoyId)
                .setResourceId(resourceId)
                .setLabels(envoyLabels)
                .setTenantId(tenantId)
                .setAddress(address);

        String identifierPath = String.format("/tenants/%s/identifiers/%s",
                tenantId, resourceId);

        envoyResourceManagement.registerResource(tenantId, envoyId, leaseId, resourceId, envoyLabels, address).join();

        verifyResourceInfo("/resources/active/" + resourceKeyHash, resourceInfo, leaseId);
        verifyResourceInfo(identifierPath, resourceInfo, null);

        envoyResourceManagement.delete(tenantId, resourceId).join();

        verifyDelete("/resources/active/" + resourceKeyHash);
        verifyDelete(identifierPath);
    }

    @Test
    public void testgetOne_butThereIsNone() throws ExecutionException, InterruptedException {
      final ResourceInfo result = envoyResourceManagement
          .getOne("t-none", "r-none")
          .get();

      assertNull(result);
    }

    private void verifyResourceInfo(String k, ResourceInfo v, Long leaseId) {
        client.getKVClient().get(buildKey(k))
                .thenApply(getResponse -> {
                    assertEquals("Only stored 1 item for key so should only receive 1",
                            1, getResponse.getCount());
                    KeyValue storedData = getResponse.getKvs().get(0);

                    String key = storedData.getKey().toString(StandardCharsets.UTF_8);
                    ResourceInfo resourceInfo;
                    try {
                        resourceInfo = objectMapper.readValue(storedData.getValue().getBytes(), ResourceInfo.class);
                    } catch (IOException e) {
                        assertNull("Any exception should cause a failure", e);
                        return false;
                    }
                    assertEquals(k, key);
                    assertEquals(v.getEnvoyId(), resourceInfo.getEnvoyId());
                    assertEquals(v.getResourceId(), resourceInfo.getResourceId());
                    assertEquals(v.getTenantId(), resourceInfo.getTenantId());
                    assertEquals(v.getAddress().getHostName(), resourceInfo.getAddress().getHostName());
                    assertEquals(v.getAddress().getPort(), resourceInfo.getAddress().getPort());
                    assertEquals(v.getLabels().size(), resourceInfo.getLabels().size());

                    if (leaseId != null) {
                        assertEquals(leaseId.longValue(), storedData.getLease());
                    }
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
