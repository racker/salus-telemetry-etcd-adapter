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
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.lease.LeaseGrantResponse;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

public class ZoneStorageTest {
  @Rule
  public final EtcdClusterResource etcd = new EtcdClusterResource("ZoneStorageTest", 1);

  @Rule
  public TestName testName = new TestName();

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
    zoneStorage.stop();
    client.close();
  }

  @Test
  public void testRegisterEnvoy() {

    final long leaseId = grantLease();

    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");
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
  public void testGetActiveEnvoyCountForZone() {
    final long leaseId = grantLease();
    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");

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
  public void testIncrementBoundMoreThanOne() {
    final long leaseId = grantLease();

    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");

    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();

    assertValueAndLease("/zones/active/t-1/zone-1/e-1", 0, leaseId);

    zoneStorage.incrementBoundCount(zone, "e-1", 12).join();

    assertValueAndLease("/zones/active/t-1/zone-1/e-1", 12, leaseId);
  }

  @Test
  public void testdecrementBoundCount_normal() {
    final long leaseId = grantLease();

    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");
    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();

    zoneStorage.incrementBoundCount(zone, "e-1", 12).join();

    assertValueAndLease("/zones/active/t-1/zone-1/e-1", 12, leaseId);

    zoneStorage.decrementBoundCount(zone, "e-1").join();

    assertValueAndLease("/zones/active/t-1/zone-1/e-1", 11, leaseId);
  }

  @Test
  public void testdecrementBoundCount_cappedAtZero() {
    final long leaseId = grantLease();

    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");
    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();

    zoneStorage.incrementBoundCount(zone, "e-1").join();

    assertValueAndLease("/zones/active/t-1/zone-1/e-1", 1, leaseId);

    zoneStorage.decrementBoundCount(zone, "e-1").join();

    assertValueAndLease("/zones/active/t-1/zone-1/e-1", 0, leaseId);

    zoneStorage.decrementBoundCount(zone, "e-1").join();

    // and capped at zero
    assertValueAndLease("/zones/active/t-1/zone-1/e-1", 0, leaseId);
  }

  @Test
  public void testdecrementBoundCount_decrementAfterRegister() {
    final long leaseId = grantLease();

    final ResolvedZone zone = createPrivateZone("t-1", "zone-1");
    zoneStorage.registerEnvoyInZone(zone, "e-1", "r-1", leaseId).join();

    zoneStorage.decrementBoundCount(zone, "e-1").join();

    // and capped at zero
    assertValueAndLease("/zones/active/t-1/zone-1/e-1", 0, leaseId);
  }

  @Test
  public void testLeastLoaded_emptyZone() {
    final ResolvedZone zone = createPrivateZone("t-none", "zone-nowhere");

    final Optional<String> leastLoaded = zoneStorage.findLeastLoadedEnvoy(zone).join();
    assertThat(leastLoaded.isPresent(), equalTo(false));
  }

  @Test
  public void testNewExpectedEnvoyResource() throws ExecutionException, InterruptedException {
    registerAndWatchExpected(createPrivateZone("t-1", "z-1"), "r-1", "t-1");
  }

  /**
   * This test simulates the scenario where the zone management application is restarting, but
   * during the down time a new envoy-resource registered in the zone.
   */
  @Test
  public void testResumingExpectedEnvoyWatch() throws ExecutionException, InterruptedException {
    final String tenant = testName.getMethodName();

    final ResolvedZone resolvedZone = createPrivateZone(tenant, "z-1");

    registerAndWatchExpected(resolvedZone, "r-1", tenant);

    // one envoy-resource has registered, now register another prior to re-watching

    final long leaseId = grantLease();
    zoneStorage.registerEnvoyInZone(resolvedZone, "e-2", "r-2", leaseId)
        .join();

    // sanity check KV content
    final GetResponse r2resp = client.getKVClient().get(
        ByteSequence.fromString(String.format("/zones/expected/%s/z-1/r-2",
            tenant
        ))
    ).get();
    assertThat(r2resp.getCount(), equalTo(1L));

    final GetResponse trackingResp = client.getKVClient().get(
        ByteSequence.fromString("/tracking/zones/expected")
    ).get();
    assertThat(trackingResp.getCount(), equalTo(1L));
    // ...and the relative revisions of the tracking key vs the registration while not watching
    assertThat(trackingResp.getKvs().get(0).getModRevision(), lessThan(r2resp.getKvs().get(0).getModRevision()));

    // Now restart watching, which should pick up from where the other ended

    final ZoneStorageListener listener = Mockito.mock(ZoneStorageListener.class);

    try (Watcher ignored = zoneStorage.watchExpectedZones(listener).get()) {

      verify(listener, timeout(5000)).handleNewEnvoyResourceInZone(resolvedZone);

    } finally {
      // watcher has been closed

      verify(listener, timeout(5000)).handleExpectedZoneWatcherClosed(notNull());

      verifyNoMoreInteractions(listener);
    }

  }

  @Test
  public void testWatchExpectedZones_reRegister() throws ExecutionException, InterruptedException {
    final String tenant = testName.getMethodName();

    final ResolvedZone resolvedZone = createPrivateZone(tenant, "z-1");

    final ZoneStorageListener listener = Mockito.mock(ZoneStorageListener.class);

    try (Watcher ignored = zoneStorage.watchExpectedZones(listener).get()) {

      final long leaseId = grantLease();

      zoneStorage.registerEnvoyInZone(resolvedZone, "e-1", "r-1", leaseId)
          .join();

      verify(listener, timeout(5000)).handleNewEnvoyResourceInZone(resolvedZone);

      zoneStorage.registerEnvoyInZone(resolvedZone, "e-2", "r-1", leaseId)
          .join();

      verify(listener, timeout(5000)).handleEnvoyResourceReassignedInZone(
          resolvedZone, "e-1", "e-2");

    } finally {
      // watcher has been closed

      // it is expected that watcher is closed with exception when closed prior to stopping the component
      verify(listener, timeout(5000)).handleExpectedZoneWatcherClosed(notNull());

      verifyNoMoreInteractions(listener);
    }

  }

  private void registerAndWatchExpected(ResolvedZone resolvedZone, String resourceId, String tenant)
      throws InterruptedException, ExecutionException {
    final ZoneStorageListener listener = Mockito.mock(ZoneStorageListener.class);

    try (Watcher ignored = zoneStorage.watchExpectedZones(listener).get()) {

      final long leaseId = grantLease();

      zoneStorage.registerEnvoyInZone(resolvedZone, "e-1", resourceId, leaseId)
          .join();

      verify(listener, timeout(5000)).handleNewEnvoyResourceInZone(resolvedZone);


    } finally {
      // watcher has been closed

      // it is expected that watcher is closed with exception when closed prior to stopping the component
      verify(listener, timeout(5000)).handleExpectedZoneWatcherClosed(notNull());

      verifyNoMoreInteractions(listener);
    }
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