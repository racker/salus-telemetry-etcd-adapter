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

import com.coreos.jetcd.Client;
import com.rackspace.salus.telemetry.etcd.EtcdProperties;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EnvoyLeaseTracking {

    private final Client etcd;
    private final EtcdProperties etcdProperties;

    private ConcurrentHashMap<String, Long> envoyLeases = new ConcurrentHashMap<>();

    @Autowired
    public EnvoyLeaseTracking(Client etcd, EtcdProperties etcdProperties) {
        this.etcd = etcd;
        this.etcdProperties = etcdProperties;
    }

  public CompletableFuture<Long> grant(String leaseName, long timeoutInSecs) {
    return etcd.getLeaseClient().grant(timeoutInSecs)
      .thenApply(leaseGrantResponse -> {
        final long leaseId = leaseGrantResponse.getID();
        envoyLeases.put(leaseName, leaseId);
        return leaseId;
      });
  }

    public CompletableFuture<Long> grant(String envoyInstanceId) {
        return grant(envoyInstanceId, etcdProperties.getEnvoyLeaseSec());
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
                    .join();
        }
        else {
            log.warn("Did not have lease for envoyInstanceId={}", envoyInstanceId);
        }
    }

    public CompletableFuture<Long> retrieve(String tenantId, String envoyInstanceId) {
        // NOTE: don't cache the resulting lease ID since this call might be used outside of the ambassador in which
        // case the lifespan of the envoyLeases map is not managed.
        return etcd.getKVClient().get(
                buildKey(Keys.FMT_ENVOYS_BY_ID,
                        tenantId, envoyInstanceId)
        )
                .thenApply(getResponse -> {
                    if (getResponse.getCount() == 0) {
                        log.warn("Unable to locate tenant={} envoyInstance={} in order to find lease",
                                tenantId, envoyInstanceId);
                        return 0L;
                    } else {
                        return getResponse.getKvs().get(0).getLease();
                    }
                });
    }

}
