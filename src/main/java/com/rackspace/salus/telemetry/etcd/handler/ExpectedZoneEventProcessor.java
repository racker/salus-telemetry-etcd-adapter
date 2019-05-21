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

import static com.rackspace.salus.telemetry.etcd.types.Keys.PTN_ZONE_EXPECTED;
import static com.rackspace.salus.telemetry.etcd.types.Keys.TRACKING_KEY_ZONE_EXPECTED;

import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.common.exception.ClosedClientException;
import com.coreos.jetcd.common.exception.ClosedWatcherException;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchEvent.EventType;
import com.coreos.jetcd.watch.WatchResponse;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorage;
import com.rackspace.salus.telemetry.etcd.services.ZoneStorageListener;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import java.util.regex.Matcher;
import lombok.extern.slf4j.Slf4j;


/**
 * This is used to handle any etcd watcher events that get triggered relating
 * to the expected zone key prefix.
 */
@Slf4j
public class ExpectedZoneEventProcessor implements Runnable {
  private Watcher zoneWatcher;
  private ZoneStorageListener listener;
  private ZoneStorage zoneStorage;

  public ExpectedZoneEventProcessor(Watcher zoneWatcher, ZoneStorageListener listener, ZoneStorage zoneStorage) {
    this.zoneWatcher = zoneWatcher;
    this.listener = listener;
    this.zoneStorage = zoneStorage;
  }

  @Override
  public void run() {
    final ByteSequence trackingKey = ByteSequence.fromString(TRACKING_KEY_ZONE_EXPECTED);

    while (zoneStorage.isRunning()) {
      try {
        final WatchResponse response = zoneWatcher.listen();

        try {
          for (WatchEvent event : response.getEvents()) {

            final EventType eventType = event.getEventType();

            switch (eventType) {
              case DELETE:
                // no action necessary
                break;
              case PUT:
                final String keyStr = event.getKeyValue().getKey().toStringUtf8();
                final Matcher matcher = PTN_ZONE_EXPECTED.matcher(keyStr);

                if (!matcher.matches()) {
                  continue;
                }
                String resourceId = matcher.group("resourceId");
                final ResolvedZone resolvedZone = ResolvedZone.fromKeyParts(
                    matcher.group("tenant"),
                    matcher.group("zoneId")
                );

                // prev KV is always populated by event, but a version=0 means it wasn't present in storage
                if (event.getPrevKV().getVersion() == 0) {
                  try {
                    listener.handleNewEnvoyResourceInZone(resolvedZone, resourceId);
                  } catch (Exception e) {
                    log.warn("Unexpected failure within listener={}", listener, e);
                  }
                } else {
                  try {
                    listener.handleEnvoyResourceReassignedInZone(
                        resolvedZone,
                        resourceId,
                        event.getPrevKV().getValue().toStringUtf8(),
                        event.getKeyValue().getValue().toStringUtf8()
                    );
                  } catch (Exception e) {
                    log.warn("Unexpected failure within listener={}", listener, e);
                  }
                }
                break;
              case UNRECOGNIZED:
                log.warn("Unknown expected watcher event seen by zone watcher");
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
