/*
 * Copyright 2019 Rackspace US, Inc.
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

package com.rackspace.salus.telemetry.etcd.services;

import static com.rackspace.salus.telemetry.etcd.EtcdUtils.buildGetResponse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.api.DeleteRangeResponse;
import com.coreos.jetcd.api.KeyValue;
import com.coreos.jetcd.api.RangeResponse;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.DeleteResponse;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.PutResponse;
import com.coreos.jetcd.options.GetOption;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.rackspace.salus.telemetry.etcd.types.AgentInstallSelector;
import com.rackspace.salus.telemetry.model.AgentRelease;
import com.rackspace.salus.telemetry.model.AgentType;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.model.OperatingSystem;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@JsonTest
public class AgentsCatalogServiceTest {

    // Disable full component scan and just import what we're testing
    @Configuration
    @Import({AgentsCatalogService.class,
        IdGeneratorImpl.class})
    public static class TestConfig {

    }

    @MockBean
    Client etcd;

    @Mock
    KV kv;

    @MockBean
    EnvoyLabelManagement labelManagement;

    @MockBean
    EnvoyLeaseTracking envoyLeaseTracking;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    AgentsCatalogService agentsCatalogService;

    @Before
    public void setUp() throws Exception {
        when(etcd.getKVClient()).thenReturn(kv);
    }

    @Test
    public void testInstallToExistingEnvoys() throws JsonProcessingException {
        final ObjectMapper objectMapper = new ObjectMapper();

        Set<String> resultingEnvoys = new HashSet<>();
        resultingEnvoys.add("l64");
        when(labelManagement.applyAgentInfoSelector(any(), any(), any(AgentInstallSelector.class)))
            .thenAnswer(invocationOnMock -> {
                return CompletableFuture.completedFuture(invocationOnMock.getArgument(2));
            });

        AgentRelease agentRelease = new AgentRelease()
                .setId("ai1")
                .setType(AgentType.FILEBEAT);

        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "LINUX");
        labels.put("arch", "X86_64");

        final AgentInstallSelector selector = new AgentInstallSelector()
                .setId("ais1")
                .setAgentReleaseId("ai1")
                .setLabels(labels);

        when(kv.get(ByteSequence.fromString("/agentsById/ai1")))
                .thenReturn(buildGetResponse(objectMapper, "/agentsById/ai1", agentRelease));

        // prep with no prior selectors
        when(kv.get(eq(ByteSequence.fromString("/tenants/t1/agentInstallSelectors/FILEBEAT")), any(GetOption.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        new GetResponse(RangeResponse.newBuilder()
                                .build())));

        when(kv.put(
                argThat(a -> a.toStringUtf8().startsWith("/tenants/t1/agentInstallSelectors/FILEBEAT/")),
                any(ByteSequence.class)
        ))
                .thenReturn(buildPutResponse());

        final CompletableFuture<AgentInstallSelector> futureResult =
                agentsCatalogService.install("t1", selector);

        futureResult.join();

        verify(labelManagement).applyAgentInfoSelector(
            eq("t1"), eq(AgentType.FILEBEAT), argThat(agentInstallSelector ->
                agentInstallSelector.getLabels().equals(labels)));
        verify(kv).get(ByteSequence.fromString("/agentsById/ai1"));
        verify(kv).get(eq(ByteSequence.fromString("/tenants/t1/agentInstallSelectors/FILEBEAT")),
            argThat(t -> t.getEndKey().get().toStringUtf8().equals("/tenants/t1/agentInstallSelectors/FILEBEAU"))
        );
        verify(kv).put(
                argThat(a -> a.toStringUtf8().startsWith("/tenants/t1/agentInstallSelectors/FILEBEAT/")),
                any(ByteSequence.class)
        );
        verifyNoMoreInteractions(labelManagement, kv);
    }

    @Test(expected = NotFoundException.class)
    public void testAgentInstall_missingRelease() throws Throwable {
        when(kv.get(ByteSequence.fromString("/agentsById/ai1")))
            .thenReturn(
                CompletableFuture.completedFuture(
                    new GetResponse(
                        RangeResponse.newBuilder()
                            .setCount(0)
                            .build()
                    )
                )
            );

        // EXECUTE

        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "LINUX");
        labels.put("arch", "X86_64");

        final AgentInstallSelector selector = new AgentInstallSelector()
            .setId("ais1")
            .setAgentReleaseId("ai1")
            .setLabels(labels);

        try {
            agentsCatalogService.install("t1", selector).join();
            fail("Expected CompletionException");
        } catch (CompletionException e) {
            throw e.getCause();
        }
    }

    private CompletableFuture<PutResponse> buildPutResponse() {
        return CompletableFuture.completedFuture(
                new PutResponse(
                        com.coreos.jetcd.api.PutResponse.newBuilder()
                        .build()
                )
        );
    }

    @Test
    public void testRemoveOldAgentInstallSelectors() {
        when(kv.get(eq(ByteSequence.fromString("/tenants/t1/agentInstallSelectors/FILEBEAT")), any(GetOption.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        new GetResponse(RangeResponse.newBuilder()
                                .addKvs(KeyValue.newBuilder()
                                        .setKey(ByteString.copyFromUtf8(
                                                "/tenants/t1/agentInstallSelectors/FILEBEAT/a"))
                                        .setValue(ByteString.copyFromUtf8("{\n" +
                                                "  \"id\": \"a\",\n" +
                                                "  \"agentReleaseId\": \"ai1\",\n" +
                                                "  \"labels\": {\n" +
                                                "    \"os\": \"LINUX\"\n" +
                                                "  }\n" +
                                                "}"))
                                        .build())
                                .addKvs(KeyValue.newBuilder()
                                        .setKey(ByteString.copyFromUtf8(
                                                "/tenants/t1/agentInstallSelectors/FILEBEAT/b"))
                                        .setValue(ByteString.copyFromUtf8("{\n" +
                                                "  \"id\": \"b\",\n" +
                                                "  \"agentReleaseId\": \"ai2\",\n" +
                                                "  \"labels\": {\n" +
                                                "    \"os\": \"WINDOWS\"\n" +
                                                "  }\n" +
                                                "}"))
                                        .build())
                                .addKvs(KeyValue.newBuilder()
                                        .setKey(ByteString.copyFromUtf8(
                                                "/tenants/t1/agentInstallSelectors/FILEBEAT/c"))
                                        .setValue(ByteString.copyFromUtf8("{\n" +
                                                "  \"id\": \"c\",\n" +
                                                "  \"agentReleaseId\": \"ai3\",\n" +
                                                "  \"labels\": {\n" +
                                                "    \"os\": \"LINUX\"\n" +
                                                "  }\n" +
                                                "}"))
                                        .build())
                                .build())));

        when(kv.delete(any(ByteSequence.class))).thenReturn(
                CompletableFuture.completedFuture(new DeleteResponse(
                        DeleteRangeResponse.newBuilder().build()
                ))
        );


        Map<String, String> labels = new HashMap<>();
        labels.put("os", OperatingSystem.LINUX.name());

        AgentInstallSelector agentInstallSelector = new AgentInstallSelector()
                .setLabels(labels);
        final CompletionStage<AgentType> result = agentsCatalogService.removeOldAgentInstallSelectors(agentInstallSelector, "t1", AgentType.FILEBEAT);

        verify(kv).get(eq(ByteSequence.fromString("/tenants/t1/agentInstallSelectors/FILEBEAT")), any(GetOption.class));
        verify(kv).delete(ByteSequence.fromString("/tenants/t1/agentInstallSelectors/FILEBEAT/a"));
        verify(kv).delete(ByteSequence.fromString("/tenants/t1/agentInstallSelectors/FILEBEAT/c"));

        verifyNoMoreInteractions(kv, labelManagement);
    }

    @Test
    public void testRemoveOldAgentInstallSelectors_noneMatch() {
        when(kv.get(eq(ByteSequence.fromString("/tenants/t1/agentInstallSelectors/FILEBEAT")), any(GetOption.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        new GetResponse(RangeResponse.newBuilder()
                                .addKvs(KeyValue.newBuilder()
                                        .setKey(ByteString.copyFromUtf8(
                                                "/tenants/t1/agentInstallSelectors/FILEBEAT/b"))
                                        .setValue(ByteString.copyFromUtf8("{\n" +
                                                "  \"id\": \"b\",\n" +
                                                "  \"agentReleaseId\": \"ai2\",\n" +
                                                "  \"labels\": {\n" +
                                                "    \"os\": \"WINDOWS\"\n" +
                                                "  }\n" +
                                                "}"))
                                        .build())
                                .build())));

        Map<String, String> labels = new HashMap<>();
        labels.put("os", OperatingSystem.LINUX.name());

        AgentInstallSelector agentInstallSelector = new AgentInstallSelector()
                .setLabels(labels);
        final CompletionStage<AgentType> result = agentsCatalogService.removeOldAgentInstallSelectors(agentInstallSelector, "t1", AgentType.FILEBEAT);

        verify(kv).get(eq(ByteSequence.fromString("/tenants/t1/agentInstallSelectors/FILEBEAT")), any(GetOption.class));

        verifyNoMoreInteractions(kv, labelManagement);
    }

    @Test
    public void testRemoveOldAgentInstallSelectors_noneExist() {
        when(kv.get(eq(ByteSequence.fromString("/tenants/t1/agentInstallSelectors/FILEBEAT")), any(GetOption.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        new GetResponse(RangeResponse.newBuilder()
                                .build())));

        Map<String, String> labels = new HashMap<>();
        labels.put("os", OperatingSystem.LINUX.name());

        AgentInstallSelector agentInstallSelector = new AgentInstallSelector()
                .setLabels(labels);
        final CompletionStage<AgentType> result = agentsCatalogService.removeOldAgentInstallSelectors(agentInstallSelector, "t1", AgentType.FILEBEAT);

        verify(kv).get(eq(ByteSequence.fromString("/tenants/t1/agentInstallSelectors/FILEBEAT")), any(GetOption.class));

        verifyNoMoreInteractions(kv, labelManagement);
    }
}
