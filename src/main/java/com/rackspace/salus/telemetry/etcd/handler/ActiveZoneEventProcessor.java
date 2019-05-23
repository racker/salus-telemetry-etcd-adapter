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

package com.rackspace.salus.telemetry.etcd.handler;

import static com.rackspace.salus.telemetry.etcd.types.Keys.PTN_ZONE_ACTIVE;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_ACTIVE;

import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.common.exception.ClosedClientException;
import com.coreos.jetcd.common.exception.ClosedWatcherException;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorageListener;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import java.util.Optional;
import java.util.regex.Matcher;
import lombok.extern.slf4j.Slf4j;

/**
 * This is used to handle any etcd watcher events that get triggered relating
 * to the active zone key prefix.
 */
@Slf4j
public class ActiveZoneEventProcessor implements Runnable {

  private Watcher zoneWatcher;
  private ZoneStorageListener listener;
  private ZoneStorage zoneStorage;

  public ActiveZoneEventProcessor(Watcher zoneWatcher, ZoneStorageListener listener, ZoneStorage zoneStorage) {
    this.zoneWatcher = zoneWatcher;
    this.listener = listener;
    this.zoneStorage = zoneStorage;
  }

  @Override
  public void run() {
    final ByteSequence trackingKey = ByteSequence.fromString(TRACKING_KEY_ZONE_ACTIVE);

    while (zoneStorage.isRunning()) {
      try {
        final WatchResponse response = zoneWatcher.listen();

        try {
          for (WatchEvent event : response.getEvents()) {

            final String keyStr = event.getKeyValue().getKey().toStringUtf8();
            final Matcher matcher = PTN_ZONE_ACTIVE.matcher(keyStr);

            if (!matcher.matches()) {
              log.warn("Unable to parse active event key={}", keyStr);
              continue;
            }

            String resourceId = matcher.group("resourceId");
            final ResolvedZone resolvedZone = ResolvedZone.fromKeyParts(
                matcher.group("tenant"),
                matcher.group("zoneName")
            );

            switch (event.getEventType()) {
              case PUT:
                // Only act on this when a new key is created
                // not when the bound monitor count is updated for the key.
                if (event.getPrevKV().getVersion() == 0) {
                  listener.handleActiveEnvoyConnection(resolvedZone, resourceId);
                }
                break;
              case DELETE:
                Optional<String> envoyId = zoneStorage.getEnvoyIdForResource(resolvedZone, resourceId).join();
                if (!envoyId.isPresent()) {
                  log.warn("No expected envoyId found for disconnected resource {}", resourceId);
                  break;
                }
                // create an expiring entry with the poller timeout.
                listener.handleActiveEnvoyDisconnection(resolvedZone, resourceId, envoyId.get());
                break;
              case UNRECOGNIZED:
                log.warn("Unknown active watcher event seen by zone watcher");
                break;
            }
          }
        } finally {
          zoneStorage.updateTrackingKey(trackingKey);
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
}