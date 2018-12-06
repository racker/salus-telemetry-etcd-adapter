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

import static com.coreos.jetcd.data.ByteSequence.fromString;
import static com.rackspace.salus.telemetry.etcd.EtcdUtils.buildKey;
import static com.rackspace.salus.telemetry.etcd.EtcdUtils.buildValue;
import static com.rackspace.salus.telemetry.etcd.EtcdUtils.completedDeletedResponse;
import static com.rackspace.salus.telemetry.etcd.EtcdUtils.parseValue;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.options.DeleteOption;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.GetOption.SortOrder;
import com.coreos.jetcd.options.GetOption.SortTarget;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.types.KeyedValue;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.model.AgentInstallSelector;
import com.rackspace.salus.telemetry.model.AgentRelease;
import com.rackspace.salus.telemetry.model.AgentType;
import com.rackspace.salus.telemetry.model.NotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
@Slf4j
public class AgentsCatalogService {

    private final Client etcd;
    private final ObjectMapper objectMapper;
    private final EnvoyLabelManagement envoyLabelManagement;
    private final IdGenerator idGenerator;

    @Autowired
    public AgentsCatalogService(Client etcd, ObjectMapper objectMapper,
                                EnvoyLabelManagement envoyLabelManagement,
                                IdGenerator idGenerator) {
        this.etcd = etcd;
        this.objectMapper = objectMapper;
        this.envoyLabelManagement = envoyLabelManagement;
        this.idGenerator = idGenerator;
    }

    public CompletableFuture<AgentRelease> declare(AgentRelease agentRelease) {
        agentRelease.setId(idGenerator.generate());

        final ByteSequence agentReleaseBytes;
        try {
            agentReleaseBytes = ByteSequence.fromBytes(objectMapper.writeValueAsBytes(agentRelease));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to marshal AgentInfo", e);
        }

        return CompletableFuture.allOf(
            etcd.getKVClient().put(
                buildKey(
                    Keys.FMT_AGENTS_BY_TYPE,
                    agentRelease.getType(), agentRelease.getVersion(), agentRelease.getId()
                ),
                agentReleaseBytes
            ),
            etcd.getKVClient().put(
                buildKey("/agentsById/{agentId}", agentRelease.getId()),
                agentReleaseBytes
            )
        )
            .thenApply(aVoid -> agentRelease);
    }

    public CompletableFuture<List<AgentRelease>> getAllAgentReleases() {
        return getAgentReleasesByPrefix(fromString("/agentsByType/"));
    }

    /**
     * Provides a query-friendly wrapper around the "getAgentReleases" operations
     * @return if id is populated, only that agent release. If type is set, only releases of for
     * that agent type. Otherwise, returns all agent releases.
     */
    public CompletableFuture<List<AgentRelease>> queryAgentReleases(@Nullable  String id, @Nullable AgentType type) {
      if (StringUtils.hasText(id)) {
        return getAgentById(id)
            .thenApply(Collections::singletonList);
      }
      else if (type != null) {
        return getAgentReleasesByType(type);
      }
      else {
        return getAllAgentReleases();
      }
    }

    public CompletableFuture<List<AgentRelease>> getAgentReleasesByType(AgentType agentType) {
        return getAgentReleasesByPrefix(buildKey("/agentsByType/{agentType}", agentType.name()));
    }

    private CompletableFuture<List<AgentRelease>> getAgentReleasesByPrefix(ByteSequence prefix) {

      return etcd.getKVClient().get(
          prefix,
          GetOption.newBuilder()
              .withPrefix(prefix)
              .withSortField(SortTarget.MOD)
              .withSortOrder(SortOrder.DESCEND)
              .build()
      )
          .thenApply((getResponse) -> {

            return getResponse.getKvs().stream()
                .map(keyValue -> {
                  try {
                    return objectMapper
                        .readValue(keyValue.getValue().getBytes(), AgentRelease.class);
                  } catch (IOException e) {
                    log.warn("Failed to parse AgentRelease", e);
                    return null;
                  }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
          });
    }

    public CompletableFuture<AgentRelease> getAgentById(String agentId) {

        return etcd.getKVClient().get(
            buildKey("/agentsById/{agentId}", agentId)
        )
            .thenApply((getResponse) -> {

                if (getResponse.getCount() == 0) {
                    throw new NotFoundException("Could not find AgentInfo");
                }
                try {
                    return parseValue(objectMapper, getResponse.getKvs().get(0), AgentRelease.class);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to parse AgentInfo", e);
                }
            });
    }

    public CompletableFuture<AgentInstallSelector> install(String tenantId, AgentInstallSelector agentInstallSelector) {

        agentInstallSelector.setId(idGenerator.generate());

        // get agent info
        // to derive agent type
        // put agent install selector

        return getTypeFromAgentInfo(agentInstallSelector.getAgentInfoId())
            .thenCompose(agentType ->
                removeOldAgentInstallSelectors(
                    agentInstallSelector, tenantId, agentType))
            .thenCompose(agentType ->
                putAgentInstallSelector(
                    agentInstallSelector, tenantId, agentType))
            .thenCompose(agentType ->
                envoyLabelManagement.applyAgentInfoSelector(
                    tenantId, agentType, agentInstallSelector));
    }

    public CompletableFuture<List<AgentInstallSelector>> getInstallations(String tenantId) {
      final ByteSequence key = buildKey(
          Keys.FMT_AGENT_INSTALL_SELECTORS_PREFIX,
          tenantId);

      return etcd.getKVClient()
          .get(
              key,
              GetOption.newBuilder()
                  .withPrefix(key)
                  .build()
          )
          .thenApply(getResponse ->
              getResponse.getKvs().stream()
                  .map(keyValue -> {

                    try {
                      return objectMapper.readValue(
                          keyValue.getValue().getBytes(),
                          AgentInstallSelector.class
                      );
                    } catch (IOException e) {
                      log.warn(
                          "Failed to parse agentIntallSelector={}",
                          keyValue.getKey().toStringUtf8()
                      );
                      return null;
                    }
                  })
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList())
          );
    }

    /**
     * Removes any agent install selectors with exactly the same labels
     *
     * @param agentInstallSelector
     * @param tenantId
     * @param agentType
     * @return
     */
    public CompletionStage<AgentType> removeOldAgentInstallSelectors(AgentInstallSelector agentInstallSelector, String tenantId, AgentType agentType) {
        final ByteSequence prefix = buildKey("/tenants/{tenant}/agentInstallSelectors/{agentType}",
            tenantId, agentType);

        return etcd.getKVClient().get(prefix, GetOption.newBuilder()
            .withPrefix(prefix)
            .build())
            .thenCompose(getResponse ->
                removeExactMatchesOfInstallSelectors(agentInstallSelector, agentType, getResponse)
            );
    }

    private CompletableFuture<AgentType> removeExactMatchesOfInstallSelectors(AgentInstallSelector agentInstallSelector,
                                                                              AgentType agentType,
                                                                              GetResponse getResponse) {
        return getResponse.getKvs().stream()
            .map(kv -> {
                try {
                    return KeyedValue.of(
                        kv.getKey(),
                        parseValue(objectMapper, kv, AgentInstallSelector.class)
                    );
                } catch (IOException e) {
                    log.warn("Unable to parse AgentInstallSelector from {}",
                        kv.getValue().toStringUtf8(), e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .filter(existing ->
                existing.getValue().getLabels().equals(agentInstallSelector.getLabels()))
            .map(existing ->
            {
                log.debug("Removing previous exact-match AgentInstallSelector at {}", existing.getKey().toStringUtf8());
                return etcd.getKVClient().delete(existing.getKey());
            })
            .reduce(EtcdUtils::byComposingCompletables)
            .orElse(completedDeletedResponse())
            .thenApply(deleteResponse -> agentType);
    }

    private CompletionStage<@NotNull AgentType> putAgentInstallSelector(AgentInstallSelector agentInstallSelector,
                                                                        String tenantId, @NotNull AgentType agentType) {
        final ByteSequence key = buildKey(
            Keys.FMT_AGENT_INSTALL_SELECTORS,
            tenantId, agentType, agentInstallSelector.getId());

        try {
            return etcd.getKVClient().put(
                key,
                buildValue(objectMapper, agentInstallSelector)
            )
                .thenApply(putResponse -> agentType);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to build AgentInfoSelector", e);
        }
    }

    private CompletableFuture<@NotNull AgentType> getTypeFromAgentInfo(@NotEmpty String agentInfoId) {
        return etcd.getKVClient()
            .get(buildKey("/agentsById/{agentId}", agentInfoId))
            .thenApply(getResponse -> {
                if (getResponse.getCount() == 0) {
                    return null;
                }

                try {
                    final AgentRelease agentRelease = parseValue(
                        objectMapper, getResponse.getKvs().get(0),
                        AgentRelease.class
                    );

                    return agentRelease.getType();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to parse AgentInfo", e);
                }
            });
    }

  public CompletableFuture<Boolean> deleteAgentRelease(String id) {
    final ByteSequence idKey = buildKey("/agentsById/{agentId}", id);

    return etcd.getKVClient()
        .delete(
            idKey,
            DeleteOption.newBuilder()
                .withPrevKV(true)
                .build()
        )
        .thenApply(resp -> {
          if (resp.getDeleted() == 0) {
            return null;
          }

          try {
            return objectMapper.readValue(resp.getPrevKvs().get(0).getValue().getBytes(), AgentRelease.class);
          } catch (IOException e) {
            log.warn("Failed to parse agentsById entry at id={}", id);
            return null;
          }
        })
        .thenCompose(agentRelease -> {
          if (agentRelease == null) {
            // short circuit the follow up deletion below with an immediate false
            return CompletableFuture.completedFuture(false);
          }

          return etcd.getKVClient()
              .delete(
                  buildKey(
                      Keys.FMT_AGENTS_BY_TYPE,
                      agentRelease.getType(), agentRelease.getVersion(), agentRelease.getId()
                  )
              )
              .thenApply(deleteResponse -> deleteResponse.getDeleted() != 0);
        });
  }
}
