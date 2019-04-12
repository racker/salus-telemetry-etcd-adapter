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

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.GetOption.SortOrder;
import com.coreos.jetcd.options.GetOption.SortTarget;
import com.coreos.jetcd.options.PutOption;
import com.rackspace.salus.telemetry.etcd.types.EtcdStorageException;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ZoneStorage {

  private static final String FMT_BOUND_COUNT = "%010d";

  private final Client etcd;

  @Autowired
  public ZoneStorage(Client etcd) {
    this.etcd = etcd;
  }

  @SuppressWarnings("UnstableApiUsage")
  public CompletableFuture<?> registerEnvoyInZone(ResolvedZone zone, String envoyId,
                                                  String resourceId, long envoyLeaseId) throws EtcdStorageException {
    log.debug("Registering envoy={} with resourceId={} in zone={}",
        envoyId, resourceId, zone);

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
          final int newCount = prevCount + 1;

          log.debug("Putting new bound count={} for envoy={} in zone={}", newCount, envoyId, zone);

          final ByteSequence newValue = ByteSequence.fromString(String.format(FMT_BOUND_COUNT,
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
                }
                else {
                  log.debug("Re-trying incrementing bound count of envoy={} in zone={} due to collision",
                      envoyId, zone);
                  return incrementBoundCount(zone, envoyId);
                }

              });
        });
  }

  public CompletableFuture<Optional<String>> findLeastLoadedEnvoy(ResolvedZone zone) {
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
}
