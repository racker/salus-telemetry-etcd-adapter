/*
 * Copyright 2020 Rackspace US, Inc.
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
import static com.rackspace.salus.telemetry.etcd.EtcdUtils.fromString;
import static com.rackspace.salus.telemetry.etcd.types.Keys.FMT_ZONE_ACTIVE;
import static com.rackspace.salus.telemetry.etcd.types.Keys.FMT_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.FMT_ZONE_EXPIRING;

import com.rackspace.salus.telemetry.etcd.types.EtcdStorageException;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lease.LeaseTimeToLiveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.LeaseOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.watch.WatchEvent;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Encapsulates the aspects of reading, updating, and watching the etcd aspect of zones.
 * The etcd stored aspects of zones are limited to the dynamic aspects need to track the
 * lease-bounded keys in real-time.
 */
@Service
@Slf4j
public class ZoneStorage {

  private final Client etcd;
  private EnvoyLeaseTracking envoyLeaseTracking;
  private boolean running = true;

  @Autowired
  public ZoneStorage(Client etcd,
      EnvoyLeaseTracking envoyLeaseTracking) {
    this.etcd = etcd;
    this.envoyLeaseTracking = envoyLeaseTracking;
  }

  @SuppressWarnings("UnstableApiUsage")
  public CompletableFuture<?> registerEnvoyInZone(ResolvedZone zone, String envoyId,
                                                  String resourceId, long envoyLeaseId)
      throws EtcdStorageException {
    log.debug("Registering envoy={} with resourceId={} in zone={}",
        envoyId, resourceId, zone
    );

    final KV kv = etcd.getKVClient();

    return CompletableFuture.allOf(
        kv.put(
            buildKey(FMT_ZONE_ACTIVE, zone.getTenantForKey(), zone.getZoneNameForKey(), resourceId),
            // value isn't used, so just put a marker value so it shows up in etcdctl debugging
            fromString("-"),
            PutOption.newBuilder()
                .withLeaseId(envoyLeaseId)
                .build()
        ),
        kv.put(
            buildKey(FMT_ZONE_EXPECTED, zone.getTenantForKey(), zone.getZoneNameForKey(), resourceId),
            fromString(envoyId),
            PutOption.DEFAULT
        )
    );
  }

  public CompletableFuture<List<String>> getActivePollerResourceIdsInZone(ResolvedZone zone) {
    final ByteSequence prefix =
        buildKey(FMT_ZONE_ACTIVE, zone.getTenantForKey(), zone.getZoneNameForKey(), "");

    return etcd.getKVClient()
        .get(
            prefix,
            GetOption.newBuilder()
                .withPrefix(prefix)
                .build()
        )
        .thenApply(getResponse ->
            getResponse.getKvs().stream()
                .map(kv -> {
                  final String key = kv.getKey().toString(StandardCharsets.UTF_8);
                  return key.substring(key.lastIndexOf("/") + 1);
                })
                .collect(Collectors.toList())
        );
  }

  public CompletableFuture<Long> getActiveEnvoyCountForZone(ResolvedZone zone) {
      final ByteSequence prefix =
              buildKey(FMT_ZONE_ACTIVE, zone.getTenantForKey(), zone.getZoneNameForKey(), "");

      return etcd.getKVClient().get(
              prefix,
              GetOption.newBuilder()
                      .withPrefix(prefix)
                      .withCountOnly(true)
                      .build()
      ).thenApply(GetResponse::getCount);
  }

  /**
   * Returns a mapping of envoy id to resource id for the provided zone.
   * @param zone The zone to get all envoy id -> resource id mappings.
   * @return The mappings found for the zone.
   */
  public CompletableFuture<Map<String, String>> getEnvoyIdToResourceIdMap(
      ResolvedZone zone) {
    final ByteSequence prefix =
        buildKey(FMT_ZONE_EXPECTED, zone.getTenantForKey(), zone.getZoneNameForKey(), "");

    return etcd.getKVClient().get(
        prefix,
        GetOption.newBuilder()
            .withPrefix(prefix)
            .build()
    ).thenApply(getResponse -> {

      final Map<String, String> mapping = new HashMap<>();

      for (KeyValue kv : getResponse.getKvs()) {
        final String key = kv.getKey().toString(StandardCharsets.UTF_8);
        final String resourceId = key.substring(key.lastIndexOf("/") + 1);
        final String envoyId = kv.getValue().toString(StandardCharsets.UTF_8);

        mapping.put(envoyId, resourceId);
      }

      return mapping;
    });

  }

  /**
   * Returns a mapping of poller-envoy resourceId to envoyId for the provided zone.
   * @param zone The zone to get all envoy id -> resource id mappings.
   * @return The mappings found for the zone.
   */
  public CompletableFuture<Map<String, String>> getResourceIdToEnvoyIdMap(
      ResolvedZone zone) {
    return getEnvoyIdToResourceIdMap(zone)
        .thenApply(envoyToResourceMap ->
            envoyToResourceMap.entrySet().stream()
            .collect(Collectors.toMap(
                // flip the key <- value
                Entry::getValue,
                Entry::getKey
            )));
  }

  /**
   * Retrieves the latest written revision of the events key.
   *
   * See {@link com.rackspace.salus.telemetry.etcd.types.Keys} for a description of how the tracking
   * key is used.
   *
   * @return A completable future containing the revision, or 0 if the key is not found.
   */
  public CompletableFuture<Long> getRevisionOfKey(String key) {

    final ByteSequence trackingKey = fromString(key);

    return etcd.getKVClient().txn()
        .If(
            // if the tracking key exists?
            new Cmp(trackingKey, Cmp.Op.GREATER, CmpTarget.version(0))
        )
        .Then(
            // then get the revision of that key
            Op.get(trackingKey, GetOption.DEFAULT)
        )
        .Else(
            // ...otherwise, create the tracking key since we need to bootstrap the tracking key
            // on very first startup. In production, this is a one-time event, but also enables
            // seamless development testing.
            Op.put(trackingKey, fromString(""), PutOption.DEFAULT)
        )
        .commit()
        .thenApply(txnResponse -> {
          if (txnResponse.isSucceeded()) {
            return txnResponse.getGetResponses().get(0).getKvs().get(0).getModRevision() + 1;
          } else {
            return 0L;
          }
        });
  }

  public CompletableFuture<PutResponse> createExpiringEntry(ResolvedZone zone, String resourceId, String envoyId, long pollerTimeout) {
    log.debug("Creating expired entry for zone={} with timeout={}", zone, pollerTimeout);
    String leaseName = String.format("expiring-%s:%s", zone.getTenantId(), resourceId);
    return envoyLeaseTracking.grant(leaseName, pollerTimeout)
        .thenCompose(leaseId ->
            etcd.getKVClient().put(
              buildKey(FMT_ZONE_EXPIRING, zone.getTenantForKey(), zone.getZoneNameForKey(),
                  resourceId),
              fromString(envoyId),
              PutOption.newBuilder()
                  .withLeaseId(leaseId)
                  .build()));
  }

  public CompletableFuture<DeleteResponse> removeExpiringEntry(ResolvedZone zone, String resourceId) {
    return etcd.getKVClient().delete(
        buildKey(FMT_ZONE_EXPIRING, zone.getTenantForKey(), zone.getZoneNameForKey(), resourceId));
  }

  public CompletableFuture<DeleteResponse> removeExpectedEntry(ResolvedZone zone, String resourceId) {
    return etcd.getKVClient().delete(
        buildKey(FMT_ZONE_EXPECTED, zone.getTenantForKey(), zone.getZoneNameForKey(), resourceId));
  }

  public CompletableFuture<Optional<String>> getEnvoyIdForResource(ResolvedZone zone, String resourceId) {
    return etcd.getKVClient().get(
        buildKey(FMT_ZONE_EXPECTED, zone.getTenantForKey(), zone.getZoneNameForKey(), resourceId)
    ).thenApply(getResponse -> {
      if (getResponse.getCount() > 0) {
        return Optional.of(getResponse.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8));
      } else {
        return Optional.empty();
      }
    });
  }

  /**
   * Determines whether the lease of key within a watch event is still active or not.
   * @param event The watch event to inspect.
   * @return True if the lease is no longer active, otherwise false.
   */
  public boolean isLeaseExpired(WatchEvent event) {
    log.debug("Checking for expired lease. type={} value={}", event.getEventType(), event.getPrevKV().getValue().toString(StandardCharsets.UTF_8));
    long leaseId = event.getPrevKV().getLease();
    long remainingTtl = etcd.getLeaseClient().timeToLive(leaseId, LeaseOption.DEFAULT)
        .thenApply(LeaseTimeToLiveResponse::getTTl).join();
    return remainingTtl <= 0;
  }

  /**
   * Touches the key to bump the version number associated to it.
   * This is only used for tracking keys, since we only input an empty string.
   * @param key The tracking key to act on.
   */
  public void incrementTrackingKeyVersion(ByteSequence key) {
    etcd.getKVClient().put(key, fromString(""));
  }

  public boolean isRunning() {
    return running;
  }

  public Watch getWatchClient() {
    return etcd.getWatchClient();
  }

  @PreDestroy
  public void stop() {
    running = false;
  }
}