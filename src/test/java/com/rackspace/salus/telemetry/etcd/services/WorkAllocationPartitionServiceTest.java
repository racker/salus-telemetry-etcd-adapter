package com.rackspace.salus.telemetry.etcd.services;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.util.KeyHashing;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import com.rackspace.salus.telemetry.etcd.types.KeyRange;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.etcd.types.WorkAllocationRealm;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
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
    client = io.etcd.jetcd.Client.builder().endpoints(
        etcd.cluster().getClientEndpoints()
    ).build();

    service = new WorkAllocationPartitionService(
        client, new KeyHashing(), objectMapper, idGenerator);
  }

  @After
  public void tearDown() throws Exception {
    client.close();
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

    final List<KeyRange> ranges = service.getPartitions(WorkAllocationRealm.PRESENCE_MONITOR)
        .get();

    assertThat(ranges, hasSize(2));
    assertEquals(
        new KeyRange()
        .setStart("0000000000000000000000000000000000000000000000000000000000000000")
        .setEnd("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        ranges.get(0)
    );
    assertEquals(
        new KeyRange()
        .setStart("8000000000000000000000000000000000000000000000000000000000000000")
        .setEnd("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        ranges.get(1)
    );
  }

  private void assertRangeAt(String expectedStart, String expectedEnd, String id)
      throws ExecutionException, InterruptedException {
    final ByteSequence key = EtcdUtils
        .buildKey(Keys.FMT_WORKALLOC_REGISTRY, WorkAllocationRealm.PRESENCE_MONITOR, id);

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