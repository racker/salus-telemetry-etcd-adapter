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
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.api.DeleteRangeResponse;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.DeleteResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.model.AgentConfig;
import com.rackspace.salus.telemetry.model.AgentType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
public class ConfigServiceTest {

    // Disable full component scan and just import what we're testing
    @Configuration
    @Import({ConfigService.class})
    public static class TestConfig {

    }

    @MockBean
    Client etcd;

    @Mock
    KV kv;

    @MockBean
    IdGenerator idGenerator;

    @MockBean
    EnvoyLabelManagement envoyLabelManagement;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    ConfigService configService;

    @Before
    public void setUp() throws Exception {
        when(etcd.getKVClient()).thenReturn(kv);
    }

    @Test
    public void testConfigService() throws JsonProcessingException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final String configKey = "/tenants/t1/agentConfigs/byId/ac1";

        when(kv.put(any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(idGenerator.generate()).thenReturn("ac1");
        when(envoyLabelManagement.applyConfig(eq("t1"), any()))
            .thenAnswer(invocationOnMock ->
                CompletableFuture.completedFuture(invocationOnMock.getArgument(1))
            );

        AgentConfig agentConfig = new AgentConfig()
            .setAgentType(AgentType.FILEBEAT)
            .setContent("config goes here")
            .setLabels(Collections.singletonMap("os", "LINUX"));
        AgentConfig agentConfig2 = new AgentConfig()
            .setAgentType(AgentType.FILEBEAT)
            .setContent("config2 goes here")
            .setLabels(Collections.singletonMap("os", "Darwin"));

        configService.create("t1", agentConfig)
            .join();

        verify(etcd).getKVClient();

        verify(kv).put(
            eq(ByteSequence.fromString(configKey)),
            any()
        );

        verify(envoyLabelManagement).applyConfig("t1", agentConfig);

        verifyNoMoreInteractions(etcd, envoyLabelManagement);

        // Test get
        final ByteSequence key = EtcdUtils.buildKey(Keys.FMT_AGENT_CONFIGS, "t1", "");
        when(kv.get(eq(key), any()))
            .thenReturn(buildGetResponse(objectMapper, configKey, agentConfig));
        List<AgentConfig> results = configService.getAll("t1").join();
        assertEquals(results, Arrays.asList(agentConfig));

        // Test modify
        when(kv.get(ByteSequence.fromString(configKey)))
            .thenReturn(buildGetResponse(objectMapper, configKey, agentConfig));
        configService.modify("t1", agentConfig2, "ac1").join();
        verify(envoyLabelManagement).applyConfig("t1", agentConfig2);
        verify(kv).get(eq(ByteSequence.fromString(configKey)));

        // Test delete
        when(envoyLabelManagement.deleteConfig(eq("t1"), any()))
            .thenAnswer(invocationOnMock ->
                CompletableFuture.completedFuture(invocationOnMock.getArgument(1))
            );
        when(kv.delete((ByteSequence.fromString(configKey))))
            .thenReturn(CompletableFuture.completedFuture(
                new DeleteResponse(DeleteRangeResponse.newBuilder()
                    .setDeleted(1)
                    .build()))
            );
        when(kv.get(ByteSequence.fromString(configKey)))
            .thenReturn(buildGetResponse(objectMapper, configKey, agentConfig2));
       configService.delete("t1", "ac1").join();
       verify(envoyLabelManagement).deleteConfig("t1", agentConfig2);
       verify(kv).delete(eq(ByteSequence.fromString(configKey)));
    }

}
