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

import io.etcd.jetcd.Client;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EnvoyLeaseTracking {

    private final Client etcd;

  private final ConcurrentHashMap<String, Long> envoyLeases = new ConcurrentHashMap<>();

    @Autowired
    public EnvoyLeaseTracking(Client etcd) {
        this.etcd = etcd;
    }

  public CompletableFuture<Long> grant(String leaseName, long timeoutInSecs) {
    return etcd.getLeaseClient().grant(timeoutInSecs)
      .thenApply(leaseGrantResponse -> {
        final long leaseId = leaseGrantResponse.getID();
        log.debug("Tracking lease={} for envoy={}", leaseId, leaseName);
        envoyLeases.put(leaseName, leaseId);
        return leaseId;
      });
  }

    public boolean keepAlive(String envoyInstanceId) {
        final Long leaseId = envoyLeases.get(envoyInstanceId);
        if (leaseId != null) {
            etcd.getLeaseClient().keepAliveOnce(leaseId).join();
            return true;
        }
        else {
            log.warn("Did not have lease for envoyInstanceId={}", envoyInstanceId);
            return false;
        }
    }

    public void revoke(String envoyInstanceId) {
        final Long leaseId = envoyLeases.get(envoyInstanceId);
        if (leaseId != null) {
            etcd.getLeaseClient().revoke(leaseId)
                    .thenAccept(leaseRevokeResponse -> {
                        log.debug("Revoked lease={} for envoy={}", leaseId, envoyInstanceId);
                    })
                    .exceptionally(throwable -> {
                      log.warn("Failed to revoke lease={} for envoy={} message={}", leaseId, envoyInstanceId, throwable.getMessage());
                      return null;
                    });
        }
        else {
            log.warn("Did not have lease for envoyInstanceId={}", envoyInstanceId);
        }
    }

}
