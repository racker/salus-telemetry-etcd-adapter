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

package com.rackspace.salus.telemetry.etcd.workpart;

import static com.rackspace.salus.telemetry.etcd.workpart.Bits.fromString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.launcher.junit.EtcdClusterResource;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.GetOption.SortOrder;
import io.etcd.jetcd.options.GetOption.SortTarget;
import io.etcd.jetcd.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Slf4j
public class WorkAllocatorTest {

  private static final int TIMEOUT = 5000;

  @ClassRule
  public final static EtcdClusterResource etcd = new EtcdClusterResource("test-etcd", 1);

  @Rule
  public TestName testName = new TestName();

  private ThreadPoolTaskScheduler taskExecutor;
  private WorkerProperties workerProperties;
  private Client client;

  private List<WorkAllocator> workAllocatorsInTest;

  @Before
  public void setUp() throws Exception {
    taskExecutor = new ThreadPoolTaskScheduler();
    taskExecutor.setPoolSize(Integer.MAX_VALUE);
    taskExecutor.setThreadNamePrefix("watchers-");
    taskExecutor.initialize();

    workerProperties = new WorkerProperties();
    workerProperties.setPrefix("/" + testName.getMethodName() + "/");

    client = Client.builder().endpoints(
        etcd.cluster().getClientEndpoints()
    ).build();

    workAllocatorsInTest = new ArrayList<>();
  }

  @After
  public void tearDown() throws Exception {
    log.info("Stopping work allocators");
    final Semaphore stopSem = new Semaphore(0);

    workAllocatorsInTest.forEach(workAllocator -> workAllocator.stop(() -> stopSem.release()));
    try {
      stopSem.tryAcquire(workAllocatorsInTest.size(), 5, TimeUnit.SECONDS);
    } finally {
      taskExecutor.shutdown();

      final ByteSequence allPrefix = fromString("/");
      client.getKVClient().delete(
          allPrefix,
          DeleteOption.newBuilder().withPrefix(allPrefix).build()
      ).get();

      client.close();
    }
  }

  private WorkAllocator register(WorkAllocator workAllocator) {
    workAllocatorsInTest.add(workAllocator);
    return workAllocator;
  }

  @Test
  public void testSingleWorkItemSingleWorker() throws InterruptedException {
    final BulkWorkProcessor workProcessor = new BulkWorkProcessor(1);
    final WorkAllocator workAllocator = register(
        new WorkAllocator(
            workerProperties, client, workProcessor, taskExecutor)
    );
    workAllocator.start();

    workAllocator.createWork("1")
        .join();

    workProcessor.hasActiveWorkItems(1, TIMEOUT);
  }

  @Test
  public void testOneWorkerExistingItems() throws InterruptedException, ExecutionException {
    final int totalWorkItems = 5;

    final BulkWorkProcessor bulkWorkProcessor = new BulkWorkProcessor(totalWorkItems);

    final WorkAllocator workAllocator = register(
        new WorkAllocator(
            workerProperties, client, bulkWorkProcessor, taskExecutor)
    );

    for (int i = 0; i < totalWorkItems; i++) {
      workAllocator.createWork(String.format("%d", i));
    }

    workAllocator.start();

    bulkWorkProcessor.hasActiveWorkItems(totalWorkItems, 2000);

    assertWorkLoad(totalWorkItems, workAllocator.getId());
  }

  @Test
  public void testGetWorkRegistry() throws InterruptedException, ExecutionException {
    final int totalWorkItems = 5;

    final WorkProcessor workProcessor = mock(WorkProcessor.class);

    final WorkAllocator workAllocator = register(
        new WorkAllocator(
            workerProperties, client, workProcessor, taskExecutor)
    );

    for (int i = 0; i < totalWorkItems; i++) {
      workAllocator.createWork(String.format("content-%d", i)).join();
    }

    final List<Work> workRegistryContent = workAllocator.getWorkRegistry()
        .get();

    assertEquals(totalWorkItems, workRegistryContent.size());
    for (Work work : workRegistryContent) {
      assertThat(work.getContent(), startsWith("content-"));
    }
  }

  @Test
  public void testOneWorkerWithItemDeletion() throws InterruptedException, ExecutionException {
    final int totalWorkItems = 5;

    final BulkWorkProcessor bulkWorkProcessor = new BulkWorkProcessor(totalWorkItems);

    final WorkAllocator workAllocator = register(
        new WorkAllocator(
            workerProperties, client, bulkWorkProcessor, taskExecutor)
    );

    final List<Work> createdWork = new ArrayList<>();
    for (int i = 0; i < totalWorkItems; i++) {
      createdWork.add(
          workAllocator.createWork(String.format("%d", i)).get()
      );
    }

    workAllocator.start();

    bulkWorkProcessor.hasActiveWorkItems(totalWorkItems, 2000);
    assertWorkLoad(totalWorkItems, workAllocator.getId());

    workAllocator.deleteWork(createdWork.get(0).getId());
    bulkWorkProcessor.hasActiveWorkItems(totalWorkItems - 1, 2000);
    assertWorkLoad(totalWorkItems - 1, workAllocator.getId());

  }

  @Test
  public void testOneWorkerJoinedByAnother() throws ExecutionException, InterruptedException {
    final int totalWorkItems = 6;

    // rebalance delay needs to be comfortably within the hasActiveWorkItems timeouts used below
    workerProperties.setRebalanceDelay(Duration.ofMillis(500));

    final BulkWorkProcessor workProcessor1 = new BulkWorkProcessor(totalWorkItems);
    final WorkAllocator workAllocator1 = register(
        new WorkAllocator(
            workerProperties, client, workProcessor1, taskExecutor)
    );

    final BulkWorkProcessor workProcessor2 = new BulkWorkProcessor(totalWorkItems);
    final WorkAllocator workAllocator2 = register(
        new WorkAllocator(
            workerProperties, client, workProcessor2, taskExecutor)
    );

    final List<Work> createdWork = new ArrayList<>();
    for (int i = 0; i < totalWorkItems; i++) {
      createdWork.add(
          workAllocator1.createWork(String.format("%d", i)).get()
      );
    }

    workAllocator1.start();
    workProcessor1.hasActiveWorkItems(totalWorkItems, TIMEOUT);

    log.info("Starting second worker");
    workAllocator2.start();
    workProcessor1.hasActiveWorkItems(totalWorkItems / 2, TIMEOUT);
    workProcessor2.hasActiveWorkItems(totalWorkItems / 2, TIMEOUT);

  }

  @Test
  public void testBulkReplaceWork() throws InterruptedException, ExecutionException {
    final int initialWorkCount = 15;
    final int newWorkCount = 10;
    final Semaphore initialStarts = new Semaphore(0);
    final Semaphore stops = new Semaphore(0);
    final Semaphore newStarts = new Semaphore(0);
    final WorkProcessor workprocessor = mock(WorkProcessor.class);

    doAnswer(invocationOnMock -> {
      final String content = invocationOnMock.getArgument(1);

      if (content.startsWith("initial-")) {
        initialStarts.release();
      } else {
        newStarts.release();
      }
      return null;
    }).when(workprocessor).start(any(), any());
    doAnswer(invocationOnMock -> {
      stops.release();
      return null;
    }).when(workprocessor).stop(any(), any());

    final WorkAllocator workAllocator1 = register(
        new WorkAllocator(
            workerProperties, client, workprocessor, taskExecutor)
    );

    final WorkAllocator workAllocator2 = register(
        new WorkAllocator(
            workerProperties, client, workprocessor, taskExecutor)
    );

    workAllocator1.start();
    workAllocator2.start();

    final List<String> initialWorkContent = IntStream.range(0, initialWorkCount)
        .mapToObj(i -> String.format("initial-%02d", i))
        .collect(Collectors.toList());
    final CompletableFuture<Boolean> bulkResult = workAllocator1.bulkReplaceWork(
        initialWorkContent
    );
    assertTrue(bulkResult.get());

    assertTrue(initialStarts.tryAcquire(initialWorkCount, TIMEOUT, TimeUnit.MILLISECONDS));

    assertWorkContent(initialWorkContent);

    final List<String> newWorkContent = IntStream.range(0, newWorkCount)
        .mapToObj(i -> "new-" + Integer.toString(i))
        .collect(Collectors.toList());
    final CompletableFuture<Boolean> bulkReplaceResult = workAllocator2.bulkReplaceWork(
        newWorkContent
    );
    assertTrue(bulkReplaceResult.get());

    assertTrue(newStarts.tryAcquire(newWorkCount, TIMEOUT, TimeUnit.MILLISECONDS));
    assertTrue(stops.tryAcquire(initialWorkCount, TIMEOUT, TimeUnit.MILLISECONDS));

    assertWorkContent(newWorkContent);
  }

  private void assertWorkContent(List<String> workContent)
      throws ExecutionException, InterruptedException {
    final ByteSequence prefix = fromString(workerProperties.getPrefix() + Bits.REGISTRY_SET);

    final GetResponse response = client.getKVClient()
        .get(prefix,
            GetOption.newBuilder()
                .withPrefix(prefix)
                .withSortField(SortTarget.VALUE)
                .withSortOrder(SortOrder.ASCEND)
                .build())
        .get();

    final List<String> actual = response.getKvs().stream()
        .map(keyValue -> keyValue.getValue().toString(StandardCharsets.UTF_8))
        .collect(Collectors.toList());

    assertEquals(workContent, actual);
  }

  @Test
  public void testBulkReplaceWork_nullForNew() throws InterruptedException, ExecutionException {
    final int workCount = 10;
    final Semaphore starts = new Semaphore(0);
    final Semaphore stops = new Semaphore(0);
    final WorkProcessor workprocessor = mock(WorkProcessor.class);

    doAnswer(invocationOnMock -> {
      starts.release();
      return null;
    }).when(workprocessor).start(any(), any());
    doAnswer(invocationOnMock -> {
      stops.release();
      return null;
    }).when(workprocessor).stop(any(), any());

    final WorkAllocator workAllocator = register(
        new WorkAllocator(
            workerProperties, client, workprocessor, taskExecutor)
    );

    for (int i = 0; i < workCount; i++) {
      workAllocator.createWork(String.format("content-%d", i)).join();
    }

    workAllocator.start();

    assertTrue(starts.tryAcquire(workCount, TIMEOUT, TimeUnit.MILLISECONDS));

    workAllocator.bulkReplaceWork(null);

    assertTrue(stops.tryAcquire(workCount, TIMEOUT, TimeUnit.MILLISECONDS));

    final List<Work> remainingWork = workAllocator.getWorkRegistry().get();
    assertThat(remainingWork, hasSize(0));
  }

  @Test
  public void testBulkReplaceWork_emptyForNew() throws InterruptedException, ExecutionException {
    final int workCount = 10;
    final Semaphore starts = new Semaphore(0);
    final Semaphore stops = new Semaphore(0);
    final WorkProcessor workprocessor = mock(WorkProcessor.class);

    doAnswer(invocationOnMock -> {
      starts.release();
      return null;
    }).when(workprocessor).start(any(), any());
    doAnswer(invocationOnMock -> {
      stops.release();
      return null;
    }).when(workprocessor).stop(any(), any());

    final WorkAllocator workAllocator = register(
        new WorkAllocator(
            workerProperties, client, workprocessor, taskExecutor)
    );

    for (int i = 0; i < workCount; i++) {
      workAllocator.createWork(String.format("content-%d", i)).join();
    }

    workAllocator.start();

    assertTrue(starts.tryAcquire(workCount, TIMEOUT, TimeUnit.MILLISECONDS));

    workAllocator.bulkReplaceWork(Collections.emptyList());

    assertTrue(stops.tryAcquire(workCount, TIMEOUT, TimeUnit.MILLISECONDS));

    final List<Work> remainingWork = workAllocator.getWorkRegistry().get();
    assertThat(remainingWork, hasSize(0));
  }

  @Test
  public void testRebalanceRounding() throws ExecutionException, InterruptedException {
    final int totalWorkItems = 4;

    // rebalance delay needs to be comfortably within the hasActiveWorkItems timeouts used below
    workerProperties.setRebalanceDelay(Duration.ofMillis(500));

    final BulkWorkProcessor workProcessor1 = new BulkWorkProcessor(totalWorkItems);
    final WorkAllocator workAllocator1 = register(
        new WorkAllocator(
            workerProperties, client, workProcessor1, taskExecutor)
    );

    final BulkWorkProcessor workProcessor2 = new BulkWorkProcessor(totalWorkItems);
    final WorkAllocator workAllocator2 = register(
        new WorkAllocator(
            workerProperties, client, workProcessor2, taskExecutor)
    );

    final BulkWorkProcessor workProcessor3 = new BulkWorkProcessor(totalWorkItems);
    final WorkAllocator workAllocator3 = register(
        new WorkAllocator(
            workerProperties, client, workProcessor3, taskExecutor)
    );

    final BulkWorkProcessor workProcessor4 = new BulkWorkProcessor(totalWorkItems);
    final WorkAllocator workAllocator4 = register(
        new WorkAllocator(
            workerProperties, client, workProcessor4, taskExecutor)
    );

    final List<Work> createdWork = new ArrayList<>();
    for (int i = 0; i < totalWorkItems; i++) {
      createdWork.add(
          workAllocator1.createWork(String.format("%d", i)).get()
      );
    }

    workAllocator1.start();
    workProcessor1.hasActiveWorkItems(totalWorkItems, TIMEOUT);

    log.info("starting second allocator");
    workAllocator2.start();
    workProcessor1.hasActiveWorkItems(totalWorkItems / 2, TIMEOUT);
    workProcessor2.hasActiveWorkItems(totalWorkItems / 2, TIMEOUT);

    log.info("starting third allocator");
    workAllocator3.start();
    // wait past rebalance to ensure load balance of 1.3333 is rounded to 2
    Thread.sleep(600);
    // The expected balancing looks uneven here, but it's because the work load has a rounding-up
    // tolerance to avoid the one work item, in this case, from flip-flopping between the allocators
    workProcessor1.hasActiveWorkItems(2, TIMEOUT);
    workProcessor2.hasActiveWorkItems(2, TIMEOUT);
    workProcessor3.hasActiveWorkItems(0, TIMEOUT);

    log.info("adding new work for third to pickup");
    createdWork.add(
        workAllocator1.createWork(String.format("%d", totalWorkItems + 1)).get()
    );
    workProcessor3.hasActiveWorkItems(1, TIMEOUT);
    workProcessor1.hasActiveWorkItems(2, TIMEOUT);
    workProcessor2.hasActiveWorkItems(2, TIMEOUT);
  }

  @Test
  public void testTwoWorkersReleasedToOne() throws InterruptedException, ExecutionException {
    final int totalWorkItems = 6;

    workerProperties.setRebalanceDelay(Duration.ofMillis(500));

    final BulkWorkProcessor workProcessor1 = new BulkWorkProcessor(totalWorkItems);
    final WorkAllocator workAllocator1 = register(
        new WorkAllocator(
            workerProperties, client, workProcessor1, taskExecutor)
    );
    workAllocator1.start();

    final BulkWorkProcessor workProcessor2 = new BulkWorkProcessor(totalWorkItems);
    final WorkAllocator workAllocator2 = register(
        new WorkAllocator(
            workerProperties, client, workProcessor2, taskExecutor)
    );
    workAllocator2.start();

    // both are started and ready to accept newly created worked items

    final List<Work> createdWork = new ArrayList<>();
    for (int i = 0; i < totalWorkItems; i++) {
      createdWork.add(
          workAllocator1.createWork(String.format("%d", i)).get()
      );
    }

    // first verify even load across the two
    workProcessor1.hasActiveWorkItems(totalWorkItems / 2, TIMEOUT);
    workProcessor2.hasActiveWorkItems(totalWorkItems / 2, TIMEOUT);

    log.info("stopping allocator 2");
    final Semaphore stopped = new Semaphore(0);
    workAllocator2.stop(() -> {
      stopped.release();
    });

    stopped.acquire();
    workProcessor1.hasActiveWorkItems(totalWorkItems, TIMEOUT);
    workProcessor2.hasActiveWorkItems(0, TIMEOUT);
  }

  private void assertWorkLoad(int expected, String workAllocatorId)
      throws ExecutionException, InterruptedException {
    final int actualWorkLoad = client.getKVClient()
        .get(
            fromString(workerProperties.getPrefix() + Bits.WORKERS_SET + workAllocatorId)
        )
        .thenApply(getResponse -> {
          final int actual = Integer
              .parseInt(
                  getResponse.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8), 10);
          return actual;
        })
        .get();

    assertEquals(expected, actualWorkLoad);
  }

  @Test
  public void testManyWorkersMoreWorkItems() throws InterruptedException, ExecutionException {
    final int totalWorkers = 5;
    final int totalWorkItems = 40;

    final BulkWorkProcessor bulkWorkProcessor = new BulkWorkProcessor(totalWorkItems);

    final List<WorkAllocator> workAllocators = IntStream.range(0, totalWorkers)
        .mapToObj(index -> register(
            new WorkAllocator(workerProperties, client, bulkWorkProcessor, taskExecutor)))
        .collect(Collectors.toList());

    for (WorkAllocator workAllocator : workAllocators) {
      workAllocator.start();
    }
    // allow them to start and settle
    Thread.sleep(1000);

    final List<Work> createdWork = new ArrayList<>();
    for (int i = 0; i < totalWorkItems; i++) {
      createdWork.add(
          workAllocators.get(0).createWork(String.format("%d", i)).get()
      );
    }

    bulkWorkProcessor.hasEnoughStarts(totalWorkItems, 10000);

    bulkWorkProcessor.hasActiveWorkItems(totalWorkItems, 10000);

    final ByteSequence activePrefix = fromString(workerProperties.getPrefix() + Bits.ACTIVE_SET);
    final WorkItemSummary workItemSummary = client.getKVClient()
        .get(
            activePrefix,
            GetOption.newBuilder()
                .withPrefix(activePrefix)
                .build()
        )
        .thenApply(getResponse -> {
          final WorkItemSummary result = new WorkItemSummary();
          for (KeyValue kv : getResponse.getKvs()) {
            result.activeWorkIds.add(Bits.extractIdFromKey(kv));
            final String workerId = kv.getValue().toString(StandardCharsets.UTF_8);
            int load = result.workerLoad.getOrDefault(workerId, 0);
            result.workerLoad.put(workerId, load + 1);
          }
          return result;
        })
        .get();

    assertThat(workItemSummary.activeWorkIds, hasSize(totalWorkItems));
    //
    workItemSummary.workerLoad.forEach((workerId, load) -> {
      log.info("Worker={} has load={}", workerId, load);
    });
  }

  @Test
  public void testVersionZeroAssumption() throws ExecutionException, InterruptedException {
    final ByteSequence resultKey = fromString("/result");
    final TxnResponse resp = client.getKVClient().txn()
        .If(new Cmp(fromString("/doesnotexist"), Cmp.Op.EQUAL, CmpTarget.version(0)))
        .Then(Op.put(resultKey, fromString("success"),
            PutOption.DEFAULT
        ))
        .commit()
        .get();

    assertTrue(resp.isSucceeded());

    final GetResponse getResponse = client.getKVClient().get(resultKey)
        .get();

    assertEquals(
        "success",
        getResponse.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8)
    );
  }

  @Test
  public void testAlwaysTrueTxnAssumption() throws ExecutionException, InterruptedException {
    final ByteSequence resultKey = fromString("/alwaysTrue");
    final TxnResponse resp = client.getKVClient().txn()
        .Then(Op.put(resultKey, fromString("true"),
            PutOption.DEFAULT
        ))
        .commit()
        .get();

    assertTrue(resp.isSucceeded());

    final GetResponse getResponse = client.getKVClient().get(resultKey)
        .get();

    assertEquals(
        "true",
        getResponse.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8)
    );
  }

  static class WorkItemSummary {

    HashSet<String> activeWorkIds = new HashSet<>();
    Map<String, Integer> workerLoad = new HashMap<>();
  }

  private static class BulkWorkProcessor implements WorkProcessor {

    final Lock lock = new ReentrantLock();
    final Condition hasEnoughStarts = lock.newCondition();
    final Set<Integer> observedStarts;
    int activeWorkItems = 0;
    final Condition activeItemsCondition = lock.newCondition();

    public BulkWorkProcessor(int expectedWorkItems) {
      observedStarts = new HashSet<>(expectedWorkItems);
    }

    @Override
    public void start(String id, String content) {
      final int workItemIndex = Integer.parseInt(content);

      lock.lock();
      ++activeWorkItems;
      activeItemsCondition.signal();
      try {
        if (observedStarts.add(workItemIndex)) {
          hasEnoughStarts.signal();
        }
      } finally {
        lock.unlock();
      }
    }

    public void hasEnoughStarts(int expectedWorkItems, long timeout) throws InterruptedException {
      final long start = System.currentTimeMillis();

      lock.lock();
      try {
        while (observedStarts.size() < expectedWorkItems) {
          if (System.currentTimeMillis() - start > timeout) {
            Assert.fail(String.format("failed to see %d in time, had %d", expectedWorkItems,
                observedStarts.size()
            ));
          }
          hasEnoughStarts.await(timeout, TimeUnit.MILLISECONDS);
        }
      } finally {
        lock.unlock();
      }
    }

    public void hasActiveWorkItems(int expected, long timeout) throws InterruptedException {
      final long start = System.currentTimeMillis();
      lock.lock();
      try {
        while (activeWorkItems != expected) {
          if (System.currentTimeMillis() - start > timeout) {
            Assert.fail(String
                .format("hasActiveWorkItems failed to see in time %d, had %d", expected,
                    activeWorkItems
                ));
          }
          activeItemsCondition.await(timeout, TimeUnit.MILLISECONDS);
        }
      } finally {
        lock.unlock();
      }
    }

    public void hasAtLeastWorkItems(int expected, long timeout) throws InterruptedException {
      final long start = System.currentTimeMillis();
      lock.lock();
      try {
        while (activeWorkItems < expected) {
          if (System.currentTimeMillis() - start > timeout) {
            Assert
                .fail(String.format("failed to see %d in time, had %d", expected, activeWorkItems));
          }
          activeItemsCondition.await(timeout, TimeUnit.MILLISECONDS);
        }
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void update(String id, String content) {
      // ignore for now
    }

    @Override
    public void stop(String id, String content) {
      lock.lock();
      try {
        --activeWorkItems;
        activeItemsCondition.signal();
      } finally {
        lock.unlock();
      }
    }
  }
}
