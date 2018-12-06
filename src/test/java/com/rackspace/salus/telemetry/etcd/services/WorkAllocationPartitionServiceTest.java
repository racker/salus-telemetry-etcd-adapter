package com.rackspace.salus.telemetry.etcd.services;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.etcd.types.KeyRange;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.etcd.types.WorkAllocationRealm;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WorkAllocationPartitionServiceTest {

  @Rule
  public final EtcdClusterResource etcd = new EtcdClusterResource("WorkAllocationPartitionServiceTest", 1);

  @Mock
  IdGenerator idGenerator;

  ObjectMapper objectMapper = new ObjectMapper();

  WorkAllocationPartitionService service;

  Client client;

  @Before
  public void setUp() throws Exception {
    final List<String> endpoints = etcd.cluster().getClientEndpoints().stream()
        .map(URI::toString)
        .collect(Collectors.toList());
    client = com.coreos.jetcd.Client.builder().endpoints(endpoints).build();

    service = new WorkAllocationPartitionService(
        client, new KeyHashing(), objectMapper, idGenerator);
  }

  @Test
  public void testChangePartitions() throws ExecutionException, InterruptedException {

    final AtomicInteger id = new AtomicInteger(1);
    when(idGenerator.generate())
        .then(invocationOnMock -> Integer.toString(id.getAndIncrement()));

    final Boolean result = service.changePartitions(WorkAllocationRealm.PRESENCE_MONITOR, 2)
        .get();

    assertTrue(result);

    assertRangeAt(
        "0000000000000000000000000000000000000000000000000000000000000000",
        "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "1");
    assertRangeAt(
        "8000000000000000000000000000000000000000000000000000000000000000",
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "2");
  }

  private void assertRangeAt(String expectedStart, String expectedEnd, String id)
      throws ExecutionException, InterruptedException {
    final ByteSequence key = EtcdUtils
        .buildKey(Keys.FMT_WORKALLOC_PARTITIONS, WorkAllocationRealm.PRESENCE_MONITOR, id);

    final KeyRange keyRange = client.getKVClient()
        .get(key)
        .thenApply(getResponse -> {
          assertThat(getResponse.getKvs(), hasSize(1));

          try {
            return objectMapper
                .readValue(getResponse.getKvs().get(0).getValue().getBytes(), KeyRange.class);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).get();

    assertEquals(expectedStart, keyRange.getStart());
    assertEquals(expectedEnd, keyRange.getEnd());
  }
}