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
import static com.rackspace.salus.telemetry.etcd.types.Keys.FMT_ZONE_ACTIVE;
import static com.rackspace.salus.telemetry.etcd.types.Keys.FMT_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.FMT_ZONE_EXPIRING;
import static com.rackspace.salus.telemetry.etcd.types.Keys.PREFIX_ZONE_ACTIVE;
import static com.rackspace.salus.telemetry.etcd.types.Keys.PREFIX_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.PREFIX_ZONE_EXPIRING;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_ACTIVE;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_EXPIRING;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.DeleteResponse;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.PutResponse;
import com.coreos.jetcd.lease.LeaseTimeToLiveResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.GetOption.SortOrder;
import com.coreos.jetcd.options.GetOption.SortTarget;
import com.coreos.jetcd.options.LeaseOption;
import com.coreos.jetcd.options.PutOption;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.options.WatchOption.Builder;
import com.coreos.jetcd.watch.WatchEvent;
import com.rackspace.salus.telemetry.etcd.handler.ActiveZoneEventProcessor;
import com.rackspace.salus.telemetry.etcd.handler.ExpectedZoneEventProcessor;
import com.rackspace.salus.telemetry.etcd.handler.ExpiringZoneEventProcessor;
import com.rackspace.salus.telemetry.etcd.types.EtcdStorageException;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import com.rackspace.salus.telemetry.etcd.types.EnvoyResourcePair;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

/**
 * Encapsulates the aspects of reading, updating, and watching the etcd aspect of zones.
 * The etcd stored aspects of zones are limited to the dynamic aspects need to track the
 * lease-bounded keys in real-time.
 */
@Service
@Slf4j
public class ZoneStorage {

  private static final String FMT_BOUND_COUNT = "%010d";

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
            ByteSequence.fromString(String.format(FMT_BOUND_COUNT, 0)),
            PutOption.newBuilder()
                .withLeaseId(envoyLeaseId)
                .build()
        ),
        kv.put(
            buildKey(FMT_ZONE_EXPECTED, zone.getTenantForKey(), zone.getZoneNameForKey(), resourceId),
            ByteSequence.fromString(envoyId),
            PutOption.DEFAULT
        )
    );
  }

  public CompletableFuture<Integer> incrementBoundCount(ResolvedZone zone, String resourceId) {
    return incrementBoundCount(zone, resourceId, 1);
  }

  public CompletableFuture<Integer> decrementBoundCount(ResolvedZone zone, String resourceId) {
    return incrementBoundCount(zone, resourceId, -1);
  }

  public CompletableFuture<Integer> incrementBoundCount(ResolvedZone zone, String resourceId,
                                                        int amount) {
    log.debug("Incrementing bound count of resource={} in zone={}", resourceId, zone);

    final ByteSequence key = buildKey(
        FMT_ZONE_ACTIVE, zone.getTenantForKey(), zone.getZoneNameForKey(), resourceId);

    return etcd.getKVClient().get(key)
        .thenCompose(getResponse -> {

          if (getResponse.getKvs().isEmpty()) {
            throw new EtcdStorageException(
                String.format("Expected key not present: %s", key.toStringUtf8()));
          }

          final KeyValue kvEntry = getResponse.getKvs().get(0);
          final int prevCount = Integer.parseInt(kvEntry.getValue().toStringUtf8(), 10);
          final int newCount = constrainNewCount(prevCount + amount, zone, resourceId);

          log.debug("Putting new bound count={} for resource={} in zone={}", newCount, resourceId, zone);

          final ByteSequence newValue = ByteSequence.fromString(String.format(
              FMT_BOUND_COUNT,
              newCount
          ));

          return etcd.getKVClient().txn()
              .If(
                  new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(kvEntry.getVersion()))
              )
              .Then(
                  Op.put(
                      key,
                      newValue,
                      PutOption.newBuilder()
                          .withLeaseId(kvEntry.getLease())
                          .build()
                  )
              )
              .commit()
              .thenCompose(txnResponse -> {

                // the txn is never successful if the above If condition is not satisfied
                if (txnResponse.isSucceeded()) {
                  return CompletableFuture.completedFuture(newCount);
                } else {
                  log.debug(
                      "Re-trying incrementing bound count of resource={} in zone={} due to collision",
                      resourceId, zone
                  );
                  return incrementBoundCount(zone, resourceId, amount);
                }

              });
        });
  }

  private int constrainNewCount(int newCount,
                                ResolvedZone zone, String resourceId) {
    if (newCount >= 0) {
      return newCount;
    }
    // avoid negative counts
    else {
      // but log them since they shouldn't happen
      log.warn("Prevented negative bound count for zone={} resource={}", zone, resourceId);
      return 0;
    }
  }

  /**
   * Determines which envoy has the least amount of bound monitors assigned to it.
   * @param zone The resolved zone to search for an envoy.
   * @return The envoyId/resourceId pair for the envoy that has the least amount of bound monitors.
   */
  public CompletableFuture<Optional<EnvoyResourcePair>> findLeastLoadedEnvoy(ResolvedZone zone) {
    log.debug("Finding least loaded envoy in zone={}", zone);

    final ByteSequence prefix =
        buildKey(FMT_ZONE_ACTIVE, zone.getTenantForKey(), zone.getZoneNameForKey(), "");

    return etcd.getKVClient().get(
        prefix,
        GetOption.newBuilder()
            .withPrefix(prefix)
            .withSortField(SortTarget.VALUE)
            .withSortOrder(SortOrder.ASCEND)
            .withLimit(1)
            .build()
    )
        .thenApply(getResponse -> {
          if (getResponse.getKvs().isEmpty()) {
            return null;
          } else {
            final String envoyKeyInZone = getResponse.getKvs().get(0).getKey().toStringUtf8();
            return envoyKeyInZone.substring(envoyKeyInZone.lastIndexOf("/") + 1);
          }
        })
        .thenApply(resourceId -> {
          if (resourceId == null) {
            return Optional.empty();
          } else {

            Optional<String> envoyId = getEnvoyIdForResource(zone, resourceId).join();
            if (!envoyId.isPresent()) {
              return Optional.empty();
            } else {
              EnvoyResourcePair pair = new EnvoyResourcePair()
                  .setEnvoyId(envoyId.get())
                  .setResourceId(resourceId);

              return Optional.of(pair);
            }
          }
        });
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
   * Retrieves the latest written revision of the events key.
   *
   * See {@link com.rackspace.salus.telemetry.etcd.types.Keys} for a description of how the tracking
   * key is used.
   *
   * @return A completable future containing the revision, or 0 if the key is not found.
   */
  private CompletableFuture<Long> getRevisionOfKey(String key) {

    final ByteSequence trackingKey = ByteSequence.fromString(key);

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
            Op.put(trackingKey, ByteSequence.fromString(""), PutOption.DEFAULT)
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

  /**
   * Sets up asynchronous watching of the expected zone key range.
   *
   * @param listener the listener that will be invoked when changes related to expected zones occur
   * @return a future that is completed when the watcher is setup and being processed. The contained
   * {@link Watcher} is provided only for testing/informational purposes.
   */
  public CompletableFuture<Watcher> watchExpectedZones(ZoneStorageListener listener) {
    return watchZones(TRACKING_KEY_ZONE_EXPECTED, PREFIX_ZONE_EXPECTED, listener);
  }

  public CompletableFuture<Watcher> watchActiveZones(ZoneStorageListener listener) {
    return watchZones(TRACKING_KEY_ZONE_ACTIVE, PREFIX_ZONE_ACTIVE, listener);
  }

  public CompletableFuture<Watcher> watchExpiringZones(ZoneStorageListener listener) {
    return watchZones(TRACKING_KEY_ZONE_EXPIRING, PREFIX_ZONE_EXPIRING, listener);
  }

  public CompletableFuture<Watcher> watchZones(String trackingKey, String watchPrefixStr, ZoneStorageListener listener) {
    Assert.notNull(listener, "A ZoneStorageListener is required");

    // first we need to see if a previous app was watching the zones
    return
        getRevisionOfKey(trackingKey)
            .thenApply(watchRevision -> {
              log.debug("Watching {} from revision {}", watchPrefixStr, watchRevision);

              final ByteSequence watchPrefix = ByteSequence
                  .fromString(watchPrefixStr);

              final Builder watchOptionBuilder = WatchOption.newBuilder()
                  .withPrefix(watchPrefix)
                  .withPrevKV(true)
                  .withRevision(watchRevision);

              final Watcher zoneWatcher = etcd.getWatchClient().watch(
                  watchPrefix,
                  watchOptionBuilder.build()
              );

              switch (trackingKey) {
                case TRACKING_KEY_ZONE_EXPECTED:
                  new Thread(
                      new ExpectedZoneEventProcessor(zoneWatcher, listener, this),
                      "expectedZoneWatcher")
                      .start();
                  break;
                case TRACKING_KEY_ZONE_ACTIVE:
                  new Thread(
                      new ActiveZoneEventProcessor(zoneWatcher, listener, this),
                      "activeZoneWatcher")
                      .start();
                  break;
                case TRACKING_KEY_ZONE_EXPIRING:
                  new Thread(
                      new ExpiringZoneEventProcessor(zoneWatcher, listener, this),
                      "expiringZoneWatcher")
                      .start();
                  break;
                default:
                  log.error("Attempting to process unknown tracking key: {}", trackingKey);
                  break;
              }

              return zoneWatcher;
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
              ByteSequence.fromString(envoyId),
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
        return Optional.of(getResponse.getKvs().get(0).getValue().toStringUtf8());
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
    log.debug("Checking for expired lease. type={} value={}", event.getEventType(), event.getPrevKV().getValue().toStringUtf8());
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
    etcd.getKVClient().put(key, ByteSequence.fromString(""));
  }

  public boolean isRunning() {
    return running;
  }

  @PreDestroy
  public void stop() {
    running = false;
  }
}