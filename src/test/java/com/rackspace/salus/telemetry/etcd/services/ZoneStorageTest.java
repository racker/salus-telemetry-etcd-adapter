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

import static com.rackspace.salus.telemetry.etcd.types.ResolvedZone.createPrivateZone;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.PutResponse;
import com.coreos.jetcd.lease.LeaseGrantResponse;
import com.coreos.jetcd.options.LeaseOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchEvent.EventType;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import com.rackspace.salus.telemetry.etcd.types.EnvoyResourcePair;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

@RunWith(MockitoJUnitRunner.class)
public class ZoneStorageTest {
  @Rule
  public final EtcdClusterResource etcd = new EtcdClusterResource("ZoneStorageTest", 1);

  @Rule
  public TestName testName = new TestName();

  private Client client;

  private ZoneStorage zoneStorage;

  @Mock
  private EnvoyLeaseTracking envoyLeaseTracking;

  @Before
  public void setUp() {
    final List<String> endpoints = etcd.cluster().getClientEndpoints().stream()
        .map(URI::toString)
        .collect(Collectors.toList());
    client = com.coreos.jetcd.Client.builder().endpoints(endpoints).build();

    zoneStorage = new ZoneStorage(client, envoyLeaseTracking);
  }

  @After
  public void tearDown() {
    zoneStorage.stop();
    client.close();
  }

  @Test
  public void testRegisterEnvoy() {

    final long leaseId = grantLease();

    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");
    zoneStorage.registerEnvoyInZone(zone, "123-456", "r-1", leaseId).join();

    final GetResponse activeResponse = client.getKVClient()
        .get(ByteSequence.fromString("/zones/active/t-1/zone-1/r-1"))
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
  public void testGetActiveEnvoyCountForZone() {
    final long leaseId = grantLease();
    final ResolvedZone zone = createPrivateZone("t-1", "r-1");

    assertThat(zoneStorage.getActiveEnvoyCountForZone(zone).join(), equalTo(0L));

    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-2", "r-2", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-3", "r-3", leaseId).join();

    assertThat(zoneStorage.getActiveEnvoyCountForZone(zone).join(), equalTo(3L));
  }

  @Test
  public void testUpdateBound_and_leastLoaded() {
    // just use one lease for all three
    final long leaseId = grantLease();

    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");

    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-2", "r-2", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-3", "r-3", leaseId).join();

    for (int i = 0; i < 10; ++i) {
      zoneStorage.incrementBoundCount(zone, "r-1")
      .join();
    }
    for (int i = 0; i < 20; ++i) {
      zoneStorage.incrementBoundCount(zone, "r-2")
      .join();
    }
    for (int i = 0; i < 5; ++i) {
      zoneStorage.incrementBoundCount(zone, "r-3")
      .join();
    }

    assertValueAndLease("/zones/active/t-1/zone-1/r-1", 10, leaseId);
    assertValueAndLease("/zones/active/t-1/zone-1/r-2", 20, leaseId);
    assertValueAndLease("/zones/active/t-1/zone-1/r-3", 5, leaseId);

    final Optional<EnvoyResourcePair> leastLoaded = zoneStorage.findLeastLoadedEnvoy(zone).join();
    assertThat(leastLoaded.isPresent(), equalTo(true));
    //noinspection OptionalGetWithoutIsPresent
    assertThat(leastLoaded.get().getEnvoyId(), equalTo("e-3"));
    assertThat(leastLoaded.get().getResourceId(), equalTo("r-3"));
  }

  @Test
  public void testIncrementBoundMoreThanOne() {
    final long leaseId = grantLease();

    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");

    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();

    assertValueAndLease("/zones/active/t-1/zone-1/r-1", 0, leaseId);

    zoneStorage.incrementBoundCount(zone, "r-1", 12).join();

    assertValueAndLease("/zones/active/t-1/zone-1/r-1", 12, leaseId);
  }

  @Test
  public void testDecrementBoundCount_normal() {
    final long leaseId = grantLease();

    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");
    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();

    zoneStorage.incrementBoundCount(zone, "r-1", 12).join();

    assertValueAndLease("/zones/active/t-1/zone-1/r-1", 12, leaseId);

    zoneStorage.decrementBoundCount(zone, "r-1").join();

    assertValueAndLease("/zones/active/t-1/zone-1/r-1", 11, leaseId);
  }

  @Test
  public void testDecrementBoundCount_cappedAtZero() {
    final long leaseId = grantLease();

    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");
    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();

    zoneStorage.incrementBoundCount(zone, "r-1").join();

    assertValueAndLease("/zones/active/t-1/zone-1/r-1", 1, leaseId);

    zoneStorage.decrementBoundCount(zone, "r-1").join();

    assertValueAndLease("/zones/active/t-1/zone-1/r-1", 0, leaseId);

    zoneStorage.decrementBoundCount(zone, "r-1").join();

    // and capped at zero
    assertValueAndLease("/zones/active/t-1/zone-1/r-1", 0, leaseId);
  }

  @Test
  public void testDecrementBoundCount_decrementAfterRegister() {
    final long leaseId = grantLease();

    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");
    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();

    zoneStorage.decrementBoundCount(zone, "r-1").join();

    // and capped at zero
    assertValueAndLease("/zones/active/t-1/zone-1/r-1", 0, leaseId);
  }

  @Test
  public void testLeastLoaded_emptyZone() {
    final ResolvedZone zone = createPrivateZone("t-none", "zone-nowhere");

    final Optional<EnvoyResourcePair> leastLoaded = zoneStorage.findLeastLoadedEnvoy(zone).join();
    assertThat(leastLoaded.isPresent(), equalTo(false));
  }

  @Test
  public void testIsLeaseExpired_notExpired() {
    long leaseId = grantLease();
    com.coreos.jetcd.api.KeyValue kv = com.coreos.jetcd.api.KeyValue.newBuilder().setLease(leaseId).build();
    com.coreos.jetcd.data.KeyValue kv1 = new com.coreos.jetcd.data.KeyValue(kv);
    WatchEvent event = new WatchEvent(null, kv1, EventType.DELETE);

    assertFalse(zoneStorage.isLeaseExpired(event));
  }

  @Test
  public void testIsLeaseExpired_revoked() throws Exception {
    long leaseId = grantLease();

    com.coreos.jetcd.api.KeyValue kv = com.coreos.jetcd.api.KeyValue.newBuilder().setLease(leaseId).build();
    com.coreos.jetcd.data.KeyValue kv1 = new com.coreos.jetcd.data.KeyValue(kv);

    WatchEvent event = new WatchEvent(null, kv1, EventType.DELETE);
    revokeLease(leaseId);
    // This test passes individually without the sleep but needs to wait for the revoke to process
    // when run as part of the whole suite.
    Thread.sleep(100);

    assertTrue(zoneStorage.isLeaseExpired(event));
  }

  @Test
  public void testIsLeaseExpired_expired() throws Exception {
    long leaseId = grantLease(0);
    com.coreos.jetcd.api.KeyValue kv = com.coreos.jetcd.api.KeyValue.newBuilder().setLease(leaseId).build();
    com.coreos.jetcd.data.KeyValue kv1 = new com.coreos.jetcd.data.KeyValue(kv);
    WatchEvent event = new WatchEvent(null, kv1, EventType.DELETE);

    // this actually creates a lease of 1s, so we have to wait for it to expire.
    Thread.sleep(1000);

    assertTrue(zoneStorage.isLeaseExpired(event));
  }

  @Test
  public void testCreateExpiringEntry() throws Exception {
    final ResolvedZone zone = createPrivateZone("t-1", RandomStringUtils.randomAlphabetic(10));
    final String resourceId = RandomStringUtils.randomAlphabetic(10);
    final String envoyId = RandomStringUtils.randomAlphabetic(10);
    final long pollerTimeout = 1000;
    final long leaseId = grantLease(pollerTimeout);

    when(envoyLeaseTracking.grant(anyString(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(leaseId));

    PutResponse response = zoneStorage.createExpiringEntry(zone, resourceId, envoyId, pollerTimeout).get();

    assertFalse(response.hasPrevKv());

    final GetResponse expiringResponse = client.getKVClient()
        .get(ByteSequence.fromString(String.format("/zones/expiring/t-1/%s/%s", zone.getName(), resourceId)))
        .join();
    assertThat(expiringResponse.getKvs(), hasSize(1));

    long foundLeaseId = expiringResponse.getKvs().get(0).getLease();
    assertThat(foundLeaseId, equalTo(leaseId));

    long ttl = client.getLeaseClient().timeToLive(foundLeaseId, LeaseOption.DEFAULT).get().getGrantedTTL();
    assertThat(ttl, is(pollerTimeout));
  }

  @Test
  public void testRemoveExpiringEntry() {
    final ResolvedZone zone = createPrivateZone("t-1", RandomStringUtils.randomAlphabetic(10));
    final String resourceId = RandomStringUtils.randomAlphabetic(10);
    final String envoyId = RandomStringUtils.randomAlphabetic(10);
    final long pollerTimeout = 1000;
    final long leaseId = grantLease(pollerTimeout);

    when(envoyLeaseTracking.grant(anyString(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(leaseId));

    zoneStorage.createExpiringEntry(zone, resourceId, envoyId, pollerTimeout);

    GetResponse expiringResponse = client.getKVClient()
        .get(ByteSequence.fromString(String.format("/zones/expiring/t-1/%s/%s", zone.getName(), resourceId)))
        .join();
    assertThat(expiringResponse.getKvs(), hasSize(1));

    zoneStorage.removeExpiringEntry(zone, resourceId);

    expiringResponse = client.getKVClient()
        .get(ByteSequence.fromString(String.format("/zones/expiring/t-1/%s/%s", zone.getName(), resourceId)))
        .join();
    assertThat(expiringResponse.getKvs(), hasSize(0));
  }

  @Test
  public void testGetEnvoyIdForResource() {
    final long leaseId = grantLease();
    final String envoyId = RandomStringUtils.randomAlphabetic(10);
    final String resourceId = RandomStringUtils.randomAlphabetic(10);
    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");
    zoneStorage.registerEnvoyInZone(zone, envoyId, resourceId, leaseId).join();

    Optional<String> result = zoneStorage.getEnvoyIdForResource(zone, resourceId).join();
    assertTrue(result.isPresent());
    assertThat(result.get(), equalTo(envoyId));
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
    return grantLease(10000);
  }

  private long grantLease(long ttl) {
    final LeaseGrantResponse leaseGrant = client.getLeaseClient().grant(ttl).join();
    final long leaseId = leaseGrant.getID();

    return leaseId;
  }

  private void revokeLease(long leaseId) {
    client.getLeaseClient().revoke(leaseId);

  }
}