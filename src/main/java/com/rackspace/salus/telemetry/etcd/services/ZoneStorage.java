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
import static com.rackspace.salus.telemetry.etcd.types.Keys.DELIMITER;
import static com.rackspace.salus.telemetry.etcd.types.Keys.FMT_ZONE_ACTIVE;
import static com.rackspace.salus.telemetry.etcd.types.Keys.FMT_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.PREFIX_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.PTN_ZONE_EXPECTED;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.common.exception.ClosedClientException;
import com.coreos.jetcd.common.exception.ClosedWatcherException;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.GetOption.SortOrder;
import com.coreos.jetcd.options.GetOption.SortTarget;
import com.coreos.jetcd.options.PutOption;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.options.WatchOption.Builder;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchEvent.EventType;
import com.coreos.jetcd.watch.WatchResponse;
import com.rackspace.salus.telemetry.etcd.types.EtcdStorageException;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
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
  private boolean running = true;

  @Autowired
  public ZoneStorage(Client etcd) {
    this.etcd = etcd;
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
            buildKey(FMT_ZONE_ACTIVE, zone.getTenantForKey(), zone.getZoneIdForKey(), envoyId),
            ByteSequence.fromString(String.format(FMT_BOUND_COUNT, 0)),
            PutOption.newBuilder()
                .withLeaseId(envoyLeaseId)
                .build()
        ),
        kv.put(
            buildKey(FMT_ZONE_EXPECTED, zone.getTenantForKey(), zone.getZoneIdForKey(), resourceId),
            ByteSequence.fromString(envoyId),
            PutOption.DEFAULT
        )
    );
  }

  public CompletableFuture<Integer> incrementBoundCount(ResolvedZone zone, String envoyId) {
    return incrementBoundCount(zone, envoyId, 1);
  }

  public CompletableFuture<Integer> incrementBoundCount(ResolvedZone zone, String envoyId,
                                                        int size) {
    log.debug("Incrementing bound count of envoy={} in zone={}", envoyId, zone);

    final ByteSequence key = buildKey(
        FMT_ZONE_ACTIVE, zone.getTenantForKey(), zone.getZoneIdForKey(), envoyId);

    return etcd.getKVClient().get(key)
        .thenCompose(getResponse -> {

          if (getResponse.getKvs().isEmpty()) {
            throw new EtcdStorageException(
                String.format("Expected key not present: %s", key.toStringUtf8()));
          }

          final KeyValue kvEntry = getResponse.getKvs().get(0);
          final int prevCount = Integer.parseInt(kvEntry.getValue().toStringUtf8(), 10);
          final int newCount = prevCount + size;

          log.debug("Putting new bound count={} for envoy={} in zone={}", newCount, envoyId, zone);

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

                if (txnResponse.isSucceeded()) {
                  return CompletableFuture.completedFuture(newCount);
                } else {
                  log.debug(
                      "Re-trying incrementing bound count of envoy={} in zone={} due to collision",
                      envoyId, zone
                  );
                  return incrementBoundCount(zone, envoyId, size);
                }

              });
        });
  }

  public CompletableFuture<Optional<String>> findLeastLoadedEnvoy(ResolvedZone zone) {
    log.debug("Finding least loaded envoy in zone={}", zone);

    final ByteSequence prefix =
        buildKey(FMT_ZONE_ACTIVE, zone.getTenantForKey(), zone.getZoneIdForKey(), "");

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
            return Optional.empty();
          } else {
            final String envoyKeyInZone = getResponse.getKvs().get(0).getKey().toStringUtf8();

            return Optional.of(
                envoyKeyInZone.substring(envoyKeyInZone.lastIndexOf("/") + 1)
            );

          }
        });
  }

  private CompletableFuture<Long> determineWatchSequenceOfExpectedZones() {
    final ByteSequence trackingKey = ByteSequence.fromString(PREFIX_ZONE_EXPECTED);

    return etcd.getKVClient().txn()
        .If(
            // if the prefix tracking key exists?
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
    Assert.notNull(listener, "A ZoneStorageListener is required");

    // first we need to see if a previous app was watching the zones
    return
        determineWatchSequenceOfExpectedZones()
            .thenApply(watchRevision -> {
              // We need to append the delimiter to the watch path; otherwise, the range-watch would
              // have picked up our own touches to the tracking key.
              final String watchPrefixStr = PREFIX_ZONE_EXPECTED + DELIMITER;

              log.debug("Watching {} from revision {}", watchPrefixStr, watchRevision);

              final ByteSequence watchPrefix = ByteSequence
                  .fromString(watchPrefixStr);

              final Builder watchOptionBuilder = WatchOption.newBuilder()
                  .withPrefix(watchPrefix)
                  .withPrevKV(true)
                  .withRevision(watchRevision);

              final Watcher zoneExpectedWatcher = etcd.getWatchClient().watch(
                  watchPrefix,
                  watchOptionBuilder.build()
              );

              new Thread(() -> {
                processZoneExpectedWatcher(zoneExpectedWatcher, listener);
              }, "zoneExpectedWatcher")
                  .start();

              return zoneExpectedWatcher;
            });
  }

  private void processZoneExpectedWatcher(Watcher zoneExpectedWatcher,
                                          ZoneStorageListener listener) {
    final ByteSequence trackingKey = ByteSequence.fromString(PREFIX_ZONE_EXPECTED);

    while (running) {
      try {
        final WatchResponse response = zoneExpectedWatcher.listen();

        try {
          for (WatchEvent event : response.getEvents()) {

            final EventType eventType = event.getEventType();

            if (eventType == EventType.PUT) {

              final String keyStr = event.getKeyValue().getKey().toStringUtf8();
              final Matcher matcher = PTN_ZONE_EXPECTED.matcher(keyStr);

              if (matcher.matches()) {
                final ResolvedZone resolvedZone = ResolvedZone.fromKeyParts(
                    matcher.group("tenant"),
                    matcher.group("zoneId")
                );

                // prev KV is always populated by event, but a version=0 means it wasn't present in storage
                if (event.getPrevKV().getVersion() == 0) {
                  try {
                    listener.handleNewEnvoyResourceInZone(resolvedZone);
                  } catch (Exception e) {
                    log.warn("Unexpected failure within listener={}", listener, e);
                  }
                } else {
                  try {
                    listener.handleEnvoyResourceReassignedInZone(
                        resolvedZone,
                        event.getPrevKV().getValue().toStringUtf8(),
                        event.getKeyValue().getValue().toStringUtf8()
                    );
                  } catch (Exception e) {
                    log.warn("Unexpected failure within listener={}", listener, e);
                  }
                }
              } else {
                log.warn("Unable to parse key={}", keyStr);
              }

            }
          }
        } finally {
          etcd.getKVClient().put(trackingKey, ByteSequence.fromString(""));
        }

      } catch (InterruptedException e) {
        log.warn("Interrupted while watching zone expected", e);
      } catch (ClosedWatcherException | ClosedClientException e) {
        log.debug("Stopping processing due to closure", e);
        listener.handleExpectedZoneWatcherClosed(e);
        return;
      }
    }

    listener.handleExpectedZoneWatcherClosed(null);

  }

  @PreDestroy
  public void stop() {
    running = false;
  }
}
