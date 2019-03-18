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

import static com.rackspace.salus.telemetry.etcd.EtcdUtils.buildKey;
import static com.rackspace.salus.telemetry.etcd.EtcdUtils.completedDeletedResponse;
import static com.rackspace.salus.telemetry.etcd.EtcdUtils.completedPutResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.Txn;
import com.coreos.jetcd.api.KeyValue;
import com.coreos.jetcd.api.RangeResponse;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.options.GetOption;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.types.AgentInstallSelector;
import com.rackspace.salus.telemetry.model.AgentType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.validation.constraints.NotEmpty;
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
public class EnvoyLabelManagementTest {
    // Disable full component scan and just import what we're testing
    @Configuration
    @Import({EnvoyLabelManagement.class})
    public static class TestConfig {

    }

    @MockBean
    Client etcd;

    @Mock
    KV kv;

    @MockBean
    private EnvoyLeaseTracking envoyLeaseTracking;

    @Autowired
    private ObjectMapper objectMapper;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    EnvoyLabelManagement envoyLabelManagement;

    @Before
    public void setUp() throws Exception {
        when(etcd.getKVClient()).thenReturn(kv);
    }

    @Test
    public void testLocateNormal() throws ExecutionException, InterruptedException, JsonProcessingException {
        // Simulate three envoys all supporting FILEBEAT agent:
        // l32 = 32bit linux
        // l64 = 64bit linux
        // w64 = 64bit windows

        when(kv.get(eq(buildKey("/tenants/t1/envoysByLabel/os:LINUX")), any(GetOption.class)))
            .thenReturn(buildGetByLabelResp("t1", "os", "LINUX",
                "l32", "l64"));
        when(kv.get(eq(buildKey("/tenants/t1/envoysByLabel/arch:X86_64")), any(GetOption.class)))
            .thenReturn(buildGetByLabelResp("t1", "arch", "X86_64",
                "w64", "l64"));
        when(kv.get(eq(buildKey("/tenants/t1/envoysByAgent/FILEBEAT")), any(GetOption.class)))
            .thenReturn(buildGetByAgentResp("t1", AgentType.FILEBEAT,
                "w64", "l64", "l32"));

        final Map<String, String> labels = new HashMap<>();
        labels.put("os", "LINUX");
        labels.put("arch", "X86_64");

        final CompletableFuture<Set<String>> result =
            envoyLabelManagement.locateEnvoys("t1", AgentType.FILEBEAT, labels);

        final Set<String> resultSet = result.get();

        final HashSet<String> expected = new HashSet<>();
        expected.add("l64");
        assertEquals(expected, resultSet);

        when(envoyLeaseTracking.retrieve(eq("t1"), anyString()))
            .thenReturn(CompletableFuture.completedFuture(1000L));

    }

    private CompletableFuture<GetResponse> buildGetByLabelResp(String tenantId, String name, String value, String... envoyIds) {
        final RangeResponse.Builder builder = RangeResponse.newBuilder();
        builder.setCount(envoyIds.length);

        for (String envoyId : envoyIds) {

            builder.addKvs(
                KeyValue.newBuilder()
                    .setKey(
                        EtcdUtils.buildByteString("/tenants/%s/envoysByLabel/%s:%s/%s",
                            tenantId, name, value, envoyId)
                    )
                    .setValue(ByteString.copyFromUtf8(envoyId))
                    .build()
            );

        }

        return CompletableFuture.completedFuture(
            new GetResponse(builder.build())
        );
    }

    private CompletableFuture<GetResponse> buildGetByAgentResp(String tenantId, AgentType agentType, String... envoyIds) {
        final RangeResponse.Builder builder = RangeResponse.newBuilder();
        builder.setCount(envoyIds.length);

        for (String envoyId : envoyIds) {

            builder.addKvs(
                KeyValue.newBuilder()
                    .setKey(
                        buildKey("/tenants/{tenant}/envoysByAgent/{agentType}/{envoyInstanceId}",
                            tenantId, agentType, envoyId).getByteString()
                    )
                    .setValue(ByteString.copyFromUtf8(envoyId))
                    .build()
            );

        }

        return CompletableFuture.completedFuture(
            new GetResponse(builder.build())
        );
    }

    @Test
    public void testRegisterAndSpreadEnvoy() {
        Map<String, String> envoyLabels = new HashMap<>();
        envoyLabels.put("os", "LINUX");
        envoyLabels.put("arch", "X86_64");

        when(kv.put(argThat(t -> t.toStringUtf8().startsWith("/tenants")), any(), any()))
            .thenReturn(completedPutResponse());
        envoyLabelManagement.registerAndSpreadEnvoy("t1", "e1", "summ",
                50, envoyLabels, Arrays.asList("FILEBEAT")).join();

        verifyPut("/tenants/t1/envoysById/e1", "summ", 50L);
        verifyPut("/tenants/t1/envoysByLabel/os:LINUX/e1", "e1", 50L);
        verifyPut("/tenants/t1/envoysByLabel/arch:X86_64/e1", "e1", 50L);
        verifyPut("/tenants/t1/envoysByAgent/FILEBEAT/e1", "e1", 50L);
    }
    private void verifyPut(String k, String v, Long leaseId) {
        verify(kv).put(
                eq(ByteSequence.fromString(k)),
                eq(ByteSequence.fromString(v)),
                argThat(putOption -> putOption.getLeaseId() == leaseId));
    }

    @Test
    public void testApplyAgentInfoSelector() {

        when(envoyLeaseTracking.retrieve(eq("t1"), anyString()))
            .thenReturn(CompletableFuture.completedFuture(1000L));

        when(kv.get(eq(buildKey("/tenants/t1/envoysByLabel/os:LINUX")), any(GetOption.class)))
            .thenReturn(buildGetByLabelResp("t1", "os", "LINUX",
                "l32", "l64"));
        when(kv.get(eq(buildKey("/tenants/t1/envoysByAgent/FILEBEAT")), any(GetOption.class)))
            .thenReturn(buildGetByAgentResp("t1", AgentType.FILEBEAT,
                "l64", "l32"));

        when(kv.put(argThat(t -> t.toStringUtf8().startsWith("/agentInstalls")), any(), any()))
            .thenReturn(completedPutResponse());

        final Map<String, String> selectedLabels = new HashMap<>();
        selectedLabels.put("os", "LINUX");

        AgentInstallSelector ais = new AgentInstallSelector()
            .setId("ais1")
            .setAgentReleaseId("info1")
            .setLabels(selectedLabels);

        final AgentInstallSelector result = envoyLabelManagement.applyAgentInfoSelector("t1", AgentType.FILEBEAT, ais).join();

        assertEquals(ais, result);

        verify(kv).get(eq(buildKey("/tenants/t1/envoysByLabel/os:LINUX")),
            argThat(t -> t.getEndKey().get().toStringUtf8().equals("/tenants/t1/envoysByLabel/os:LINUY")));

        verifyPut("/agentInstalls/t1/l32/FILEBEAT", "info1", 1000L);
        verifyPut("/agentInstalls/t1/l64/FILEBEAT", "info1", 1000L);
    }

    @Test
    public void testPullAgentInstallSelectors() {

        final ByteSequence prefix = ByteSequence.fromString("/tenants/t1/agentInstallSelectors");
        when(kv.get(eq(prefix), argThat(t -> t.getEndKey().get().toStringUtf8().equals("/tenants/t1/agentInstallSelectort"))))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResponse(RangeResponse.newBuilder()
                    .setCount(1)
                    .addKvs(KeyValue.newBuilder()
                        .setModRevision(500L)
                        .setKey(
                            ByteString.copyFromUtf8("/tenants/t1/agentInstallSelectors/FILEBEAT/sel1")
                        )
                        .setValue(ByteString.copyFromUtf8("{\n" +
                            "  \"id\": \"a\",\n" +
                            "  \"agentReleaseId\": \"ai1\",\n" +
                            "  \"labels\": {\n" +
                            "    \"os\": \"LINUX\"\n" +
                            "  }\n" +
                            "}"))
                        .build())
                    .build()))
            );

        final Txn txn = mock(Txn.class);
        when(kv.txn()).thenReturn(txn);
        when(txn.If(any())).thenReturn(txn);
        when(txn.Then(any())).thenReturn(txn);
        when(txn.commit()).thenReturn(
            EtcdUtils.completedTxnResponse()
        );

        Map<String, String> envoyLabels = new HashMap<>();
        envoyLabels.put("os", "LINUX");
        envoyLabels.put("arch", "X86_64");
        envoyLabelManagement.pullAgentInstallsForEnvoy("t1", "l64", 1000L,
            Arrays.asList("FILEBEAT"), envoyLabels)
            .join();

        verify(kv).get(eq(prefix), argThat(t -> t.getEndKey().get().toStringUtf8().equals("/tenants/t1/agentInstallSelectort")));
        verify(kv).txn();
        verify(txn).If(any());
        verify(txn).Then(any());
        verify(txn).commit();

        verifyNoMoreInteractions(kv, txn);
    }

    @Test
    public void testPullAgentInstallSelectors_mismatch() {

        final ByteSequence prefix = ByteSequence.fromString("/tenants/t1/agentInstallSelectors");
        when(kv.get(eq(prefix), argThat(t -> t.getEndKey().get().toStringUtf8().equals("/tenants/t1/agentInstallSelectort"))))
            .thenReturn(CompletableFuture.completedFuture(
                new GetResponse(RangeResponse.newBuilder()
                    .setCount(1)
                    .addKvs(KeyValue.newBuilder()
                        .setModRevision(500L)
                        .setKey(
                            ByteString.copyFromUtf8("/tenants/t1/agentInstallSelectors/FILEBEAT/sel1")
                        )
                        .setValue(ByteString.copyFromUtf8("{\n" +
                            "  \"id\": \"a\",\n" +
                            "  \"agentReleaseId\": \"ai1\",\n" +
                            "  \"labels\": {\n" +
                            "    \"os\": \"LINUX\"\n" +
                            "  }\n" +
                            "}"))
                        .build())
                    .build()))
            );

        final Txn txn = mock(Txn.class);

        Map<String, String> envoyLabels = new HashMap<>();
        envoyLabels.put("os", "WINDOWS");
        envoyLabels.put("arch", "X86_64");
        envoyLabelManagement.pullAgentInstallsForEnvoy("t1", "w64", 1000L,
            Arrays.asList("FILEBEAT"), envoyLabels)
            .join();

        verify(kv).get(eq(prefix), argThat(t -> t.getEndKey().get().toStringUtf8().equals("/tenants/t1/agentInstallSelectort")));
        verifyNoMoreInteractions(kv, txn);
    }

    @Test
    public void testSortingSelectors_sameLabels() {

        @NotEmpty Map<String, String> labels = new HashMap<>();
        labels.put("os", "LINUX");

        final EnvoyLabelManagement.SelectorWithKV selectorNewer =
            new EnvoyLabelManagement.SelectorWithKV(
                new AgentInstallSelector().setLabels(labels), buildKeyValue(1000L)
            );

        final EnvoyLabelManagement.SelectorWithKV selectorOlder =
            new EnvoyLabelManagement.SelectorWithKV(
                new AgentInstallSelector().setLabels(labels), buildKeyValue(500L)
            );

        final List<EnvoyLabelManagement.SelectorWithKV> results = Stream.of(selectorOlder, selectorNewer)
            .sorted()
            .collect(Collectors.toList());

        assertSame(selectorNewer, results.get(0));
        assertSame(selectorOlder, results.get(1));
    }

    @Test
    public void testSortingSelectors_byLabels() {

        final HashMap<String, String> labelsLarge = new HashMap<>();
        labelsLarge.put("os", "LINUX");
        labelsLarge.put("arch", "X86_64");
        final EnvoyLabelManagement.SelectorWithKV selectorLarger =
            new EnvoyLabelManagement.SelectorWithKV(
                // purposely older rev to ensure sorting by label
                new AgentInstallSelector().setLabels(labelsLarge), buildKeyValue(1L)
            );

        @NotEmpty Map<String, String> labelsSmall = new HashMap<>();
        labelsSmall.put("os", "LINUX");
        final EnvoyLabelManagement.SelectorWithKV selectorSmaller =
            new EnvoyLabelManagement.SelectorWithKV(
                new AgentInstallSelector().setLabels(labelsSmall), buildKeyValue(1100L)
            );

        final List<EnvoyLabelManagement.SelectorWithKV> results = Stream.of(selectorSmaller, selectorLarger)
            .sorted()
            .collect(Collectors.toList());

        assertSame(selectorLarger, results.get(0));
        assertSame(selectorSmaller, results.get(1));
    }

    private com.coreos.jetcd.data.KeyValue buildKeyValue(long revision) {
        return new com.coreos.jetcd.data.KeyValue(
            KeyValue.newBuilder()
                .setCreateRevision(revision)
                .setModRevision(revision)
                .build()
        );
    }
}
