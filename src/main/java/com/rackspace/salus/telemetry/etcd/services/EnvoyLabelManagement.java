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
import static com.rackspace.salus.telemetry.etcd.EtcdUtils.buildValue;
import static com.rackspace.salus.telemetry.etcd.EtcdUtils.completedDeletedResponse;
import static com.rackspace.salus.telemetry.etcd.EtcdUtils.completedPutResponse;
import static com.rackspace.salus.telemetry.etcd.EtcdUtils.mapContainsAll;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Txn;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.PutResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.types.AppliedConfig;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.model.AgentConfig;
import com.rackspace.salus.telemetry.model.AgentInstallSelector;
import com.rackspace.salus.telemetry.model.AgentType;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

@Service
@Slf4j
public class EnvoyLabelManagement {

    private final Client etcd;
    private final ObjectMapper objectMapper;
    private final EnvoyLeaseTracking envoyLeaseTracking;

    @Autowired
    public EnvoyLabelManagement(Client etcd, ObjectMapper objectMapper, EnvoyLeaseTracking envoyLeaseTracking) {
        this.etcd = etcd;
        this.objectMapper = objectMapper;
        this.envoyLeaseTracking = envoyLeaseTracking;
    }

    public CompletableFuture<Set<String>> locateEnvoys(String tenantId, AgentType agentType, Map<String, String> labels) {
        Assert.hasLength(tenantId, "Requires tenantId");
        Assert.notNull(agentType, "Requires agentType");

        // This combination of stream and completion stages retrieves envoys by each label-tuple
        // and then reduces the intersection of those envoy instance IDs
        return Stream.concat(
            locateSetsOfEnvoysByLabel(tenantId, labels),
            locateSetsOfEnvoysByAgentType(tenantId, agentType)
        )
            .reduce((stageL, stageR) ->
                // and chain the completion stage of one
                stageL.thenCompose(envoyIdsL ->
                    // ...the other
                    stageR.thenApply(envoyIdsR -> {
                        Set<String> intersection = new HashSet<>(envoyIdsL);
                        // ...and compute the intersection
                        intersection.retainAll(envoyIdsR);
                        return intersection;
                    })))
            // ...and provide a default empty set since the reduce return an optional
            .orElse(CompletableFuture.completedFuture(Collections.emptySet()));
    }

    private Stream<CompletableFuture<Set<String>>> locateSetsOfEnvoysByLabel(String tenantId, Map<String, String> labels) {
        return labels.entrySet().stream()
            .map(label ->
                // do the get for a label tuple
                getEnvoysByLabel(tenantId, label.getKey(), label.getValue())
                    .thenApply(this::extractSetOfStrings));
    }

    private Stream<CompletableFuture<Set<String>>> locateSetsOfEnvoysByAgentType(String tenantId, AgentType agentType) {
        final ByteSequence prefix = buildKey("/tenants/{tenant}/envoysByAgent/{agentType}",
            tenantId, agentType);


        return Stream.of(
            etcd.getKVClient().get(
                prefix,
                GetOption.newBuilder()
                    .withPrefix(prefix)
                    .build()
            )
                .thenApply(this::extractSetOfStrings)
        );
    }

    public CompletableFuture<GetResponse> getEnvoysByLabel(String tenantId, String name, String value) {
        final ByteSequence prefix = buildKey(
            "/tenants/{tenant}/envoysByLabel/{name}:{value}",
            tenantId, name, value
        );
        return etcd.getKVClient().get(prefix,
            GetOption.newBuilder()
                .withPrefix(prefix)
                .build());
    }

    private Set<String> extractSetOfStrings(GetResponse getResponse) {
        return getResponse.getKvs().stream()
            .map(keyValue -> keyValue.getValue().toStringUtf8())
            .collect(Collectors.toSet());
    }

    public CompletableFuture<?> registerAndSpreadEnvoy(String tenantId, String envoyInstanceId, String storedSummary,
                                                       long leaseId, Map<String, String> labels, List<String> agentTypes) {

        final ByteSequence envoySummaryBytes = ByteSequence.fromString(storedSummary);
        final ByteSequence instanceIdBytes = ByteSequence.fromString(envoyInstanceId);

        final PutOption putLeaseOption = PutOption.newBuilder()
            .withLeaseId(leaseId)
            .build();

        return etcd.getKVClient().put(
            buildKey(Keys.FMT_ENVOYS_BY_ID, tenantId, envoyInstanceId),
            envoySummaryBytes,
            putLeaseOption
        )
            .thenCompose(putResponse ->
                labels.entrySet().stream()
                    .map(label ->
                        etcd.getKVClient().put(
                            buildKey(Keys.FMT_ENVOYS_BY_LABEL,
                                tenantId, label.getKey(), label.getValue(), envoyInstanceId),
                            instanceIdBytes,
                            putLeaseOption
                        ))
                    .reduce(EtcdUtils::byComposingCompletables)
                    .orElse(EtcdUtils.completedPutResponse()))
            .thenCompose(putResponse ->
                agentTypes.stream()
                    .map(agentType ->
                        etcd.getKVClient().put(
                            buildKey(Keys.FMT_ENVOYS_BY_AGENT,
                                tenantId, agentType, envoyInstanceId),
                            instanceIdBytes,
                            putLeaseOption
                        )
                    )
                    .reduce(EtcdUtils::byComposingCompletables)
                    .orElse(EtcdUtils.completedPutResponse())
            );
    }

    public CompletableFuture<AgentInstallSelector> applyAgentInfoSelector(String tenantId, AgentType agentType, AgentInstallSelector agentInstallSelector) {

        return locateEnvoys(tenantId, agentType, agentInstallSelector.getLabels())
            .thenCompose(envoyIds ->
                envoyIds.stream()
                    .map(envoyInstanceId ->
                        envoyLeaseTracking.retrieve(tenantId, envoyInstanceId)
                            .thenCompose(leaseId -> {
                                log.debug("Applying agent install of type={} leaseId={} for tenant={}, envoyInstance={}",
                                    agentType, leaseId, tenantId, envoyInstanceId);

                                final String agentInfoId = agentInstallSelector.getAgentInfoId();

                                return installAgentForExistingEnvoy(tenantId, agentType, envoyInstanceId, leaseId, agentInfoId);
                            }))
                    .reduce(EtcdUtils::byComposingCompletables)
                    .orElse(completedPutResponse())
                    .thenApply(putResponse -> agentInstallSelector));
    }

    public CompletionStage<AgentConfig> applyConfig(String tenantId, AgentConfig agentConfig) {

        final AppliedConfig appliedConfig = new AppliedConfig();
        appliedConfig.setAgentType(agentConfig.getAgentType());
        appliedConfig.setId(agentConfig.getId());
        final ByteSequence appliedConfigBytes;
        try {
            appliedConfigBytes = ByteSequence.fromBytes(objectMapper.writeValueAsBytes(appliedConfig));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to marshal AppliedConfig", e);
        }
        
        return locateEnvoys(tenantId, agentConfig.getAgentType(), agentConfig.getLabels())
            .thenCompose(envoyIds ->
                envoyIds.stream()
                    .map(envoyInstanceId ->
                        envoyLeaseTracking.retrieve(tenantId, envoyInstanceId)
                            .thenCompose(leaseId ->
                                {
                                    log.debug("Applying config={} to tenant={}, envoyInstance={}",
                                        agentConfig, tenantId, envoyInstanceId);

                                    return etcd.getKVClient().put(
                                        buildKey(Keys.FMT_APPLIED_CONFIGS,
                                            agentConfig.getSelectorScope(), tenantId, agentConfig.getId(), envoyInstanceId),
                                        appliedConfigBytes,
                                        PutOption.newBuilder()
                                            .withLeaseId(leaseId)
                                            .build()
                                    );
                                }
                            )
                    )
                    .reduce(EtcdUtils::byComposingCompletables)
                    .orElse(completedPutResponse())
                    .thenApply(putResponse -> agentConfig)
            );
    }

    public CompletableFuture<AgentConfig> deleteConfig(String tenantId, AgentConfig agentConfig) {
        return locateEnvoys(tenantId, agentConfig.getAgentType(), agentConfig.getLabels())
            .thenCompose(envoyIds ->
                envoyIds.stream()
                    .map(envoyInstanceId ->
                        {
                            log.debug("Deleting config={} to tenant={}, envoyInstance={}",
                                agentConfig.getId(), tenantId, envoyInstanceId);

                            return etcd.getKVClient().delete(
                                    buildKey(Keys.FMT_APPLIED_CONFIGS,
                                    agentConfig.getSelectorScope(),
                                        tenantId, agentConfig.getId(), envoyInstanceId));
                        }                    
                    )
                    .reduce(EtcdUtils::byComposingCompletables)
                    .orElse(completedDeletedResponse())
                    .thenApply(deleteResponse -> agentConfig)
            );
    }
    
    /**
     * Locates agent install selectors that apply to a newly attached envoy and installs highest precedent of each
     *
     * @param tenantId            the tenant
     * @param envoyInstanceId     the envoy instance to potential get the installs
     * @param leaseId             the lease ID for the envoy instance
     * @param supportedAgentTypes
     * @param envoyLabels         the labels from the envoy summary
     * @return a completable integer that is the number of install selectors that were installed
     */
    public CompletableFuture<Integer> pullAgentInstallsForEnvoy(String tenantId, String envoyInstanceId, Long leaseId, List<String> supportedAgentTypes, Map<String, String> envoyLabels) {
        /*
        - get lease ID for envoy
        - get all selectors under /tenants/{tenant}/agentInstallSelectors
        - save off the current cluster revision so we know what changes we just made
        - sort them most-specific first (by count of labels) and tie break by newest first
        - for each
            - if envoy satisfies labels
                - using a transaction, install agent info only if the selector is newer than the install key
         */

        log.debug("Pulling agent installs for tenant={}, envoyInstance={}", tenantId, envoyInstanceId);

        final ByteSequence installSelectorsPrefix = buildKey("/tenants/{tenant}/agentInstallSelectors",
            tenantId);

        return etcd.getKVClient().get(installSelectorsPrefix,
            GetOption.newBuilder()
                .withPrefix(installSelectorsPrefix)
                .build())
            .thenCompose(selectorsResp ->
                selectorsResp.getKvs().stream()
                    .map(keyValue -> {
                        try {
                            return new SelectorWithKV(
                                objectMapper.readValue(keyValue.getValue().getBytes(), AgentInstallSelector.class),
                                keyValue
                            );
                        } catch (IOException e) {
                            log.warn("Failed to parse AgentInstallSelector from key={}, value={}",
                                keyValue.getKey().toStringUtf8(), keyValue.getValue().toStringUtf8(), e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .sorted()
                    .map(selectorWithKV ->
                        installAgentIfMatchesEnvoy(tenantId, envoyInstanceId, leaseId, supportedAgentTypes, envoyLabels, selectorWithKV))
                    .reduce((l, r) ->
                        l.thenCompose(lCount -> r.thenApply(rCount -> lCount + rCount)))
                    .orElse(CompletableFuture.completedFuture(0)));
    }

    public CompletableFuture<Integer> pullConfigsForEnvoy(String tenantId, String envoyInstanceId, Long leaseId,
                                                          List<String> supportedAgentTypes, Map<String, String> envoyLabels) {

        /*
            For each agent config
                If it is one of the supported types
                    If config applies to envoy's labels
                        Apply the config to this envoy

         */
        final PutOption envoyLeasePutOption = PutOption.newBuilder()
            .withLeaseId(leaseId)
            .build();

        final ByteSequence prefix = buildKey(Keys.FMT_AGENT_CONFIGS_PREFIX, tenantId);

        return etcd.getKVClient().get(
            prefix,
            GetOption.newBuilder()
                .withPrefix(prefix)
                .build()
        )
            .thenCompose(getResponse ->
                getResponse.getKvs().stream()
                    .map(keyValue -> {
                        try {
                            return objectMapper.readValue(keyValue.getValue().getBytes(), AgentConfig.class);
                        } catch (IOException e) {
                            log.warn("Failed to parse AgentConfig from key={}, value={}",
                                keyValue.getKey().toStringUtf8(), keyValue.getValue().toStringUtf8(), e);
                            return null;
                        }

                    })
                    .filter(Objects::nonNull)
                    .filter(agentConfig ->
                        supportedAgentTypes.contains(agentConfig.getAgentType().name())
                        && mapContainsAll(envoyLabels, agentConfig.getLabels()))
                    .map(agentConfig ->
                        installConfigForExistingEnvoy(tenantId, envoyInstanceId, envoyLeasePutOption, agentConfig)
                    )
                    .reduce(EtcdUtils::byComposingCompletables)
                    .orElse(CompletableFuture.completedFuture(0))
            );
    }

    private CompletableFuture<Integer> installConfigForExistingEnvoy(String tenantId, String envoyInstanceId, PutOption envoyLeasePutOption, AgentConfig agentConfig) {
        AppliedConfig appliedConfig = new AppliedConfig()
                .setId(agentConfig.getId())
                .setAgentType(agentConfig.getAgentType());
        ByteSequence appliedConfigBytes;
        try {
            appliedConfigBytes = buildValue(objectMapper, appliedConfig);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to marshal AppliedConfig", e);
        }
            
        return etcd.getKVClient().put(
            buildKey(Keys.FMT_APPLIED_CONFIGS,
                agentConfig.getSelectorScope(), tenantId, agentConfig.getId(), envoyInstanceId),
            appliedConfigBytes,
            envoyLeasePutOption
        )
            .thenApply(putResponse -> {
                log.debug("Config={} installed to tenant={}, envoyInstance={}",
                    agentConfig, tenantId, envoyInstanceId);

                return 1;
            });
    }

    private CompletableFuture<Integer> installAgentIfMatchesEnvoy(String tenantId, String envoyInstanceId, Long leaseId,
                                                                  List<String> supportedAgentTypes,
                                                                  Map<String, String> envoyLabels,
                                                                  SelectorWithKV selectorWithKV) {
        final String selectorKey = selectorWithKV.keyValue.getKey().toStringUtf8();
        final Matcher m = Keys.PTN_AGENT_INSTALL_SELECTORS.matcher(selectorKey);
        final String agentType;
        if (m.matches()) {
            agentType = m.group("agentType");
        } else {
            log.warn("Unable to parse agentType from key={}", selectorKey);
            return CompletableFuture.completedFuture(0);
        }

        log.debug("Checking if selector={} for agentType={} applies to tenant={}, envoyInstance={}",
            selectorWithKV.selector, agentType, tenantId, envoyInstanceId);

        if (!supportedAgentTypes.contains(agentType)) {
            return CompletableFuture.completedFuture(0);
        }

        if (mapContainsAll(envoyLabels, selectorWithKV.selector.getLabels())) {
            final Txn txn = etcd.getKVClient().txn();

            final ByteSequence installKey = buildKey(Keys.FMT_AGENT_INSTALLS,
                tenantId, envoyInstanceId, agentType);
            final ByteSequence agentInfoIdBytes =
                ByteSequence.fromString(selectorWithKV.selector.getAgentInfoId());

            // only install if a more specific match wasn't already processed
            return txn.If(
                new Cmp(installKey, Cmp.Op.LESS, CmpTarget.modRevision(selectorWithKV.getRevision()))
            )
                .Then(Op.put(installKey, agentInfoIdBytes,
                    PutOption.newBuilder()
                        .withLeaseId(leaseId)
                        .build()))
                .commit()
                .thenApply(txnResponse -> {
                    final boolean installed = !txnResponse.getPutResponses().isEmpty();
                    if (installed) {
                        log.debug("Selector={} installed to tenant={}, envoyInstance={}",
                            selectorWithKV.selector, tenantId, envoyInstanceId);
                    } else {
                        log.debug("Selector={} matched, but preempted for tenant={}, envoyInstance={}",
                            selectorWithKV.selector, tenantId, envoyInstanceId);
                    }
                    return installed ? 1 : 0;
                });
        } else {
            return CompletableFuture.completedFuture(0);
        }
    }

    private CompletableFuture<PutResponse> installAgentForExistingEnvoy(String tenantId, AgentType agentType, String envoyInstanceId, Long leaseId, String agentInfoId) {
        return etcd.getKVClient().put(
            buildKey(
                Keys.FMT_AGENT_INSTALLS,
                tenantId, envoyInstanceId, agentType
            ),
            ByteSequence.fromString(agentInfoId),
            PutOption.newBuilder()
                .withLeaseId(leaseId)
                .build()
        );
    }

    @Data
    static class SelectorWithKV implements Comparable<SelectorWithKV> {
        final AgentInstallSelector selector;
        final KeyValue keyValue;

        public long getRevision() {
            return keyValue.getModRevision();
        }

        /**
         * Sorts instances by most-specific-labels first then by newest revision first, as a tie breaker.
         * Label specificity is determined by label count.
         *
         * @param o other object to compare
         * @return the usual -1, 0, 1 values
         */
        @Override
        public int compareTo(SelectorWithKV o) {
            final int ourLabelCount = this.selector.getLabels().size();
            final int otherLabelCount = o.selector.getLabels().size();
            if (ourLabelCount > otherLabelCount) {
                return -1;
            } else if (ourLabelCount < otherLabelCount) {
                return 1;
            } else {
                return Long.compare(o.getRevision(), this.getRevision());
            }
        }
    }
}
