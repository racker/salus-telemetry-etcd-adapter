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

import static com.rackspace.salus.telemetry.etcd.EtcdUtils.parseValue;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.options.GetOption;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.model.AgentConfig;
import com.rackspace.salus.telemetry.model.NotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConfigService {

    private final Client etcd;
    private final ObjectMapper objectMapper;
    private final EnvoyLabelManagement envoyLabelManagement;
    private final IdGenerator idGenerator;

    @Autowired
    public ConfigService(Client etcd, ObjectMapper objectMapper, EnvoyLabelManagement envoyLabelManagement, IdGenerator idGenerator) {
        this.etcd = etcd;
        this.objectMapper = objectMapper;
        this.envoyLabelManagement = envoyLabelManagement;
        this.idGenerator = idGenerator;
    }

    public CompletableFuture<AgentConfig> create(String tenantId, AgentConfig agentConfig) {
        return update(tenantId, agentConfig, idGenerator.generate());
    }

    public CompletableFuture<AgentConfig> modify(String tenantId, AgentConfig agentConfig, String id) {
        ByteSequence key = EtcdUtils.buildKey(Keys.FMT_AGENT_CONFIGS, tenantId, id);
        // Confirm that id exists
        getConfig(key);
        return update(tenantId, agentConfig, id);
    }

    public CompletableFuture<AgentConfig> update(String tenantId, AgentConfig agentConfig, String id) {
        agentConfig.setId(id);

        final ByteSequence agentConfigBytes;
        try {
            agentConfigBytes = ByteSequence.fromBytes(objectMapper.writeValueAsBytes(agentConfig));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to marshal AgentConfig", e);
        }

        return etcd.getKVClient().put(
            EtcdUtils.buildKey(Keys.FMT_AGENT_CONFIGS,
                tenantId, id),
            agentConfigBytes
        ).thenCompose(putResponse ->
            envoyLabelManagement.applyConfig(tenantId, agentConfig)
        ).thenApply(storedAgentConfig -> {
            log.debug("Stored agentConfig={} for tenant={}", agentConfig, tenantId);
            return storedAgentConfig;
        });
    }

    private AgentConfig getConfig(ByteSequence key) {
        AgentConfig ac;

        try {
            ac = etcd.getKVClient().get(key
            ).thenApply(getResponse -> {
                        if (getResponse.getCount() == 0) {
                            throw new NotFoundException("Could not find AgentConfig");
                        }
                        try {
                            return parseValue(objectMapper, getResponse.getKvs().get(0), AgentConfig.class);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to parse AgentConfig", e);
                        }
                    }
            ).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new NotFoundException("Failed to get AgentConfig");
        }
        return ac;
    }

    public CompletableFuture<AgentConfig> delete(String tenantId, String id) {
        ByteSequence key = EtcdUtils.buildKey(Keys.FMT_AGENT_CONFIGS, tenantId, id);
        AgentConfig ac = getConfig(key);
        return envoyLabelManagement.deleteConfig(tenantId, ac
        ).thenCompose(dummy ->
                etcd.getKVClient().delete(key
                ).thenApply(deleteResponse -> {
                    if (deleteResponse.getDeleted() == 0) {
                        return null;
                    }
                    return ac;
                })
        );
    }

    public CompletableFuture<List<AgentConfig>> getOne(String tenantId, String id) {
        ByteSequence key = EtcdUtils.buildKey(Keys.FMT_AGENT_CONFIGS, tenantId, id);
        return etcd.getKVClient().get(key)
                .thenApply(getResponse -> parseAgentConfigs(getResponse.getKvs()));
    }

    public CompletableFuture<List<AgentConfig>> getAll(String tenantId) {
        ByteSequence key = EtcdUtils.buildKey(Keys.FMT_AGENT_CONFIGS, tenantId, "");
        return etcd.getKVClient().get(key,
                GetOption.newBuilder()
                        .withPrefix(key)
                        .build())
                .thenApply(getResponse -> parseAgentConfigs(getResponse.getKvs()));
    }

    private List<AgentConfig> parseAgentConfigs(List<KeyValue> kvs) {
        return kvs.stream()
                .map(keyValue ->
                        {
                            try {
                                return parseValue(objectMapper, keyValue, AgentConfig.class);
                            } catch (IOException e) {
                                throw new RuntimeException("Failed to parse AgentConfig", e);
                            }
                        }
                )
                .collect(Collectors.toList());
    }
}
