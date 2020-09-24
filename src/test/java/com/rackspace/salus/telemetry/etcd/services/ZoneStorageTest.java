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

import static com.rackspace.salus.telemetry.etcd.EtcdUtils.fromString;
import static com.rackspace.salus.telemetry.etcd.types.Keys.FMT_ZONE_EXPIRING;
import static com.rackspace.salus.telemetry.etcd.types.ResolvedZone.createPrivateZone;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.rackspace.salus.telemetry.etcd.EtcdClusterResource;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.options.LeaseOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchEvent.EventType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
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
    client = io.etcd.jetcd.Client.builder().endpoints(
        etcd.getClientEndpoints()
    ).build();

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
        .get(fromString("/zones/active/t-1/zone-1/r-1"))
        .join();
    assertThat(activeResponse.getKvs(), hasSize(1));
    assertThat(activeResponse.getKvs().get(0).getLease(), equalTo(leaseId));
    assertThat(activeResponse.getKvs().get(0).getValue().toString(UTF_8), equalTo("-"));

    final GetResponse expectedResponse = client.getKVClient()
        .get(fromString("/zones/expected/t-1/zone-1/r-1"))
        .join();
    assertThat(expectedResponse.getKvs(), hasSize(1));
    assertThat(expectedResponse.getKvs().get(0).getLease(), equalTo(0L));
    assertThat(expectedResponse.getKvs().get(0).getValue().toString(UTF_8), equalTo("123-456"));
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
  public void testGetActivePollerResourceIdsInZone() {
    final long leaseId = grantLease();
    final ResolvedZone zone = createPrivateZone("t-1", "z-1");

    assertThat(zoneStorage.getActivePollerResourceIdsInZone(zone).join(), hasSize(0));

    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-2", "r-2", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-3", "r-3", leaseId).join();

    assertThat(zoneStorage.getActivePollerResourceIdsInZone(zone).join(),
        containsInAnyOrder("r-1", "r-2", "r-3"));
  }

  @Test
  public void testGetResourceIdToEnvoyIdMap() {
    final long leaseId = grantLease();
    final ResolvedZone zone = createPrivateZone("t-1", "z-1");

    assertThat(zoneStorage.getResourceIdToEnvoyIdMap(zone).join(), equalTo(Map.of()));

    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-2", "r-2", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-3", "r-3", leaseId).join();

    assertThat(zoneStorage.getResourceIdToEnvoyIdMap(zone).join(), equalTo(Map.of(
        "r-1", "e-1",
        "r-2", "e-2",
        "r-3", "e-3"
    )));
  }

  @Test
  public void testIsLeaseExpired_notExpired() {
    long leaseId = grantLease();
    io.etcd.jetcd.api.KeyValue kv = io.etcd.jetcd.api.KeyValue.newBuilder().setLease(leaseId).build();
    io.etcd.jetcd.KeyValue kv1 = new io.etcd.jetcd.KeyValue(kv, ByteSequence.EMPTY);
    WatchEvent event = new WatchEvent(null, kv1, EventType.DELETE);

    assertFalse(zoneStorage.isLeaseExpired(event));
  }

  @Test
  public void testIsLeaseExpired_revoked() throws Exception {
    long leaseId = grantLease();
    io.etcd.jetcd.api.KeyValue kv = io.etcd.jetcd.api.KeyValue.newBuilder().setLease(leaseId).build();
    io.etcd.jetcd.KeyValue kv1 = new io.etcd.jetcd.KeyValue(kv, ByteSequence.EMPTY);

    WatchEvent event = new WatchEvent(null, kv1, EventType.DELETE);
    revokeLease(leaseId);

    assertTrue(zoneStorage.isLeaseExpired(event));
  }

  @Test
  public void testIsLeaseExpired_expired() throws Exception {
    long leaseId = grantLease(0);
    io.etcd.jetcd.api.KeyValue kv = io.etcd.jetcd.api.KeyValue.newBuilder().setLease(leaseId).build();
    io.etcd.jetcd.KeyValue kv1 = new io.etcd.jetcd.KeyValue(kv, ByteSequence.EMPTY);
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
        .get(EtcdUtils.buildKey(FMT_ZONE_EXPIRING, "t-1", zone.getName(), resourceId))
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

    zoneStorage.createExpiringEntry(zone, resourceId, envoyId, pollerTimeout).join();

    ;
    GetResponse expiringResponse = client.getKVClient()
        .get(EtcdUtils.buildKey(FMT_ZONE_EXPIRING, "t-1", zone.getName(), resourceId))
        .join();
    assertThat(expiringResponse.getKvs(), hasSize(1));

    zoneStorage.removeExpiringEntry(zone, resourceId).join();

    expiringResponse = client.getKVClient()
        .get(fromString(String.format("/zones/expiring/t-1/%s/%s", zone.getName(), resourceId)))
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

  @Test
  public void testGetEnvoyIdToResourceIdMap() {
    final long leaseId = grantLease();
    final ResolvedZone zone = createPrivateZone("t-1", "r-1");

    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-2", "r-2", leaseId).join();
    zoneStorage.registerEnvoyInZone(zone, "e-3", "r-3", leaseId).join();

    final Map<String, String> result = zoneStorage.getEnvoyIdToResourceIdMap(zone).join();

    final Map<String, String> expected = new HashMap<>();
    expected.put("e-1", "r-1");
    expected.put("e-2", "r-2");
    expected.put("e-3", "r-3");

    assertThat(result, equalTo(expected));
  }

  private void assertValueAndLease(String key, int expectedCount, long leaseId) {
    final GetResponse getResponse = client.getKVClient().get(
        fromString(key)
    ).join();

    assertThat(getResponse.getKvs(), hasSize(1));
    assertThat(getResponse.getKvs().get(0).getValue().toString(UTF_8), equalTo(String.format("%010d", expectedCount)));
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
    client.getLeaseClient().revoke(leaseId).join();

  }
}