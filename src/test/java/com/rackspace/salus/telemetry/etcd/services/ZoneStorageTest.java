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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.lease.LeaseGrantResponse;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ZoneStorageTest {
  @Rule
  public final EtcdClusterResource etcd = new EtcdClusterResource("ZoneStorageTest", 1);

  private Client client;

  private ZoneStorage zoneStorage;

  @Before
  public void setUp() {
    final List<String> endpoints = etcd.cluster().getClientEndpoints().stream()
        .map(URI::toString)
        .collect(Collectors.toList());
    client = com.coreos.jetcd.Client.builder().endpoints(endpoints).build();

    zoneStorage = new ZoneStorage(client);
  }

  @After
  public void tearDown() {
    client.close();
  }

  @Test
  public void testRegisterEnvoy() {

    final long leaseId = grantLease();

    ResolvedZone zone = new ResolvedZone()
        .setId("zone-1")
        .setTenantId("t-1");
    zoneStorage.registerEnvoyInZone(zone, "123-456", "r-1", leaseId).join();

    final GetResponse activeResponse = client.getKVClient()
        .get(ByteSequence.fromString("/zones/active/t-1/zone-1/123-456"))
        .join();
    assertThat(activeResponse.getKvs(), hasSize(1));
    assertThat(activeResponse.getKvs().get(0).getLease(), equalTo(leaseId));
    assertThat(activeResponse.getKvs().get(0).getValue().toStringUtf8(), equalTo("0000000000"));

    final GetResponse expectedResponse = client.getKVClient()
        .get(ByteSequence.fromString("/zones/expected/t-1/zone-1/r-1"))
        .join();
    assertThat(expectedResponse.getKvs(), hasSize(1));
    assertThat(expectedResponse.getKvs().get(0).getLease(), equalTo(0L));
    assertThat(expectedResponse.getKvs().get(0).getValue().toStringUtf8(), equalTo("123-456"));
  }

  @Test
  public void testUpdateBound_and_leastLoaded() {
    // just use one lease for all three
    final long leaseId = grantLease();

    ResolvedZone zone = new ResolvedZone()
        .setId("zone-1")
        .setTenantId("t-1");

    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-2", "r-2", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-3", "r-3", leaseId).join();

    for (int i = 0; i < 10; ++i) {
      zoneStorage.incrementBoundCount(zone, "e-1")
      .join();
    }
    for (int i = 0; i < 20; ++i) {
      zoneStorage.incrementBoundCount(zone, "e-2")
      .join();
    }
    for (int i = 0; i < 5; ++i) {
      zoneStorage.incrementBoundCount(zone, "e-3")
      .join();
    }

    assertValueAndLease("/zones/active/t-1/zone-1/e-1", 10, leaseId);
    assertValueAndLease("/zones/active/t-1/zone-1/e-2", 20, leaseId);
    assertValueAndLease("/zones/active/t-1/zone-1/e-3", 5, leaseId);

    final Optional<String> leastLoaded = zoneStorage.findLeastLoadedEnvoy(zone).join();
    assertThat(leastLoaded.isPresent(), equalTo(true));
    //noinspection OptionalGetWithoutIsPresent
    assertThat(leastLoaded.get(), equalTo("e-3"));
  }

  @Test
  public void testLeastLoaded_emptyZone() {
    ResolvedZone zone = new ResolvedZone()
        .setId("zone-nowhere")
        .setTenantId("t-none");

    final Optional<String> leastLoaded = zoneStorage.findLeastLoadedEnvoy(zone).join();
    assertThat(leastLoaded.isPresent(), equalTo(false));
  }

  private void assertValueAndLease(String key, int expectedCount, long leaseId) {
    final GetResponse getResponse = client.getKVClient().get(
        ByteSequence.fromString(key)
    ).join();

    assertThat(getResponse.getKvs(), hasSize(1));
    assertThat(getResponse.getKvs().get(0).getValue().toStringUtf8(), equalTo(String.format("%010d", expectedCount)));
    assertThat(getResponse.getKvs().get(0).getLease(), equalTo(leaseId));
  }

  private long grantLease() {
    final LeaseGrantResponse leaseGrant = client.getLeaseClient().grant(10000).join();
    final long leaseId = leaseGrant.getID();
    client.getLeaseClient().keepAlive(leaseId);
    return leaseId;
  }
}