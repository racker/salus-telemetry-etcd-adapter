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

import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.kv.GetResponse;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Profile;

/**
 * Provides a Spring Boot actuator health indicator that determines the health of etcd by
 * performing a retrieval of an arbitrary key ("/"). Etcd is deemed healthy when that key
 * retrieval completes without an exception.
 * <p>
 *   This health indicator is enabled by activating the Spring profile "etcd-health-indicator"
 * </p>
 */
@Profile("etcd-health-indicator")
@Slf4j
public class EtcdHealthIndicator implements HealthIndicator {

  private final Client etcd;

  @Autowired
  public EtcdHealthIndicator(Client etcd) {
    this.etcd = etcd;
  }

  @Override
  public Health health() {
    try {
      final GetResponse resp = etcd.getKVClient().get(EtcdUtils.fromString("/"))
          .get();
      return Health.up().build();
    } catch (InterruptedException | ExecutionException e) {
      log.debug("etcd health indicator failed", e);
      return Health.down(e).build();
    }
  }
}
