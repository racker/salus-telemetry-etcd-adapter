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

import static com.rackspace.salus.telemetry.etcd.workpart.Bits.ACTIVE_SET;
import static com.rackspace.salus.telemetry.etcd.workpart.Bits.REGISTRY_SET;
import static com.rackspace.salus.telemetry.etcd.workpart.Bits.WORKERS_SET;
import static com.rackspace.salus.telemetry.etcd.workpart.Bits.WORK_LOAD_FORMAT;
import static com.rackspace.salus.telemetry.etcd.workpart.Bits.extractIdFromKey;
import static com.rackspace.salus.telemetry.etcd.workpart.Bits.fromFormat;
import static com.rackspace.salus.telemetry.etcd.workpart.Bits.fromString;
import static com.rackspace.salus.telemetry.etcd.workpart.Bits.isDeleteKeyEvent;
import static com.rackspace.salus.telemetry.etcd.workpart.Bits.isNewKeyEvent;
import static com.rackspace.salus.telemetry.etcd.workpart.Bits.isUpdateKeyEvent;
import static io.etcd.jetcd.op.Op.delete;
import static io.etcd.jetcd.op.Op.put;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.CloseableClient;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Observers;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.GetOption.SortOrder;
import io.etcd.jetcd.options.GetOption.SortTarget;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.CollectionUtils;

@Slf4j
@EnableConfigurationProperties(WorkerProperties.class)
public class WorkAllocator implements SmartLifecycle {

  private static final int EXIT_CODE_ETCD_FAILED = 1;
  private final WorkerProperties properties;
  private final Client etcd;
  private final WorkProcessor processor;
  private final TaskScheduler taskScheduler;
  private final String prefix;
  private String ourId;
  private long leaseId;
  private AtomicInteger workLoad = new AtomicInteger();
  private Semaphore workChangeSem = new Semaphore(1);
  private boolean running;
  private Deque<String> ourWork = new ConcurrentLinkedDeque<>();
  private ScheduledFuture<?> scheduledRebalance;
  private CloseableClient keepAliveClient;
  private Watcher activeWatcher;
  private Watcher registryWatcher;
  private Watcher workersWatcher;

  @Autowired
  public WorkAllocator(WorkerProperties properties, Client etcd, WorkProcessor processor,
      TaskScheduler taskScheduler) {
    this.properties = properties;
    this.etcd = etcd;
    this.processor = processor;
    this.taskScheduler = taskScheduler;

    this.prefix = properties.getPrefix().endsWith("/") ?
        properties.getPrefix() :
        properties.getPrefix() + "/";

    log.info("Using prefix={}", this.prefix);
  }

  @Override
  public int getPhase() {
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean isAutoStartup() {
    return true;
  }

  @Override
  public void start() {
    ourId = UUID.randomUUID().toString();
    log.info("Starting WorkAllocator ourId={}", ourId);

    running = true;

    etcd.getLeaseClient()
        .grant(properties.getLeaseDuration().getSeconds())
        .thenApply(leaseGrantResponse -> {
          leaseId = leaseGrantResponse.getID();
          log.info("Got lease={}, ourId={}", leaseId, ourId);
          keepAliveClient = etcd.getLeaseClient().keepAlive(leaseId, Observers.<LeaseKeepAliveResponse>builder()
              .onError(this::handleKeepAliveError)
              .build());
          return leaseId;
        })
        .thenCompose(leaseId ->
            initOurWorkerEntry()
                .thenCompose(ignored -> {
                  log.info("Starting up watchers");
                  return watchRegistry()
                      .thenAccept(o -> {
                        watchActive();
                        watchWorkers();
                      });
                }))
        .join();
  }

  private void handleKeepAliveError(Throwable throwable) {
    log.error("Error during keep alive processing", throwable);
    // Spring will gracefully shutdown via shutdown hook
    System.exit(EXIT_CODE_ETCD_FAILED);
  }

  private void handleWatcherError(Throwable throwable, String prefix) {
    log.error("Error during watch of {}", prefix, throwable);
    // Spring will gracefully shutdown via shutdown hook
    System.exit(EXIT_CODE_ETCD_FAILED);
  }

  @Override
  public void stop() {
    stop(() -> {});
  }

  @Override
  public void stop(Runnable callback) {
    if (!running) {
      callback.run();
      return;
    }

    log.info("Stopping WorkAllocator ourId={}", ourId);

    keepAliveClient.close();

    running = false;
    if (scheduledRebalance != null) {
      scheduledRebalance.cancel(false);
    }

    closeWatcher(activeWatcher);
    closeWatcher(registryWatcher);
    closeWatcher(workersWatcher);

    final Iterator<String> it = ourWork.iterator();
    while (it.hasNext()) {
      final String workId = it.next();

      getWorkContent(workId)
          .thenAccept(content -> {
            processor.stop(workId, content);
          });

      it.remove();
    }

    etcd.getLeaseClient()
        .revoke(leaseId)
        .thenAccept(resp -> {
          callback.run();
        });
  }

  private void closeWatcher(Watcher watcher) {
    if (watcher != null) {
      watcher.close();
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /**
   * Retrieve this allocator's ID for testing purpsoes.
   *
   * @return the ID assigned to this work allocator
   */
  String getId() {
    return ourId;
  }

  private Watcher buildWatcher(String prefix,
                               long revision,
                               Consumer<WatchResponse> watchResponseConsumer) {
    final ByteSequence prefixBytes = fromString(prefix);
    return etcd.getWatchClient()
        .watch(
            prefixBytes,
            WatchOption.newBuilder()
                .withRevision(revision)
                .withPrefix(prefixBytes)
                .withPrevKV(true)
                .build(),
            watchResponseConsumer,
            throwable -> handleWatcherError(throwable, prefix)
        );
  }

  public CompletableFuture<Work> createWork(String content) {
    final String id = UUID.randomUUID().toString();

    return etcd.getKVClient()
        .put(
            fromString(prefix + REGISTRY_SET + id),
            fromString(content)
        )
        .thenApply(putResponse ->
            new Work()
                .setId(id)
                .setContent(content)
                .setUpdated(putResponse.hasPrevKv())
        );
  }

  public CompletableFuture<Work> updateWork(String id, String content) {
    return etcd.getKVClient()
        .put(
            fromString(prefix + REGISTRY_SET + id),
            fromString(content)
        )
        .thenApply(putResponse ->
            new Work()
                .setId(id)
                .setContent(content)
                .setUpdated(putResponse.hasPrevKv())
        );
  }

  /**
   * @param id the work item to delete
   * @return a {@link CompletableFuture} of the number of work items successfully deleted, usually 1
   */
  public CompletableFuture<Long> deleteWork(String id) {
    return etcd.getKVClient()
        .delete(
            fromString(prefix + REGISTRY_SET + id)
        )
        .thenApply(DeleteResponse::getDeleted);
  }

  /**
   * Replaces all of the registered work items within a single operation.
   * @param contents the contents of the new work items to create
   * @return a completed value of true indicates the operation successfully removed all previous
   * work contents and registered the new ones
   */
  public CompletableFuture<Boolean> bulkReplaceWork(List<String> contents) {
    final ByteSequence registryPrefix = fromString(prefix + REGISTRY_SET);

    return etcd.getKVClient().delete(
        registryPrefix,
        DeleteOption.newBuilder()
            .withPrefix(registryPrefix)
            .build()
    ).thenCompose(deleteResponse -> {
      if (!CollectionUtils.isEmpty(contents )) {
        final Op[] ops = new Op[contents.size()];

        for (int i = 0; i < contents.size(); i++) {
          ops[i] = Op.put(
              fromString(prefix + REGISTRY_SET + UUID.randomUUID().toString()),
              fromString(contents.get(i)),
              PutOption.DEFAULT
          );
        }

        return etcd.getKVClient().txn()
            .Then(ops)
            .commit()
            .thenApply(TxnResponse::isSucceeded);
      }
      else {
        return CompletableFuture.completedFuture(true);
      }
    });
  }

  public CompletableFuture<List<Work>> getWorkRegistry() {
    final ByteSequence registryPrefix = fromString(prefix + REGISTRY_SET);

    return etcd.getKVClient().get(
        registryPrefix,
        GetOption.newBuilder()
            .withPrefix(registryPrefix)
            // the keys are UUIDs, but at least sorting will force consistent results when
            // using this operation
            .withSortField(SortTarget.KEY)
            .withSortOrder(SortOrder.ASCEND)
            .build()
    ).thenApply(getResponse ->
        getResponse.getKvs().stream()
        .map(keyValue ->
            new Work()
                .setId(keyValue.getKey().toString(StandardCharsets.UTF_8))
                .setContent(keyValue.getValue().toString(StandardCharsets.UTF_8))
        )
        .collect(Collectors.toList()));
  }

  private void watchActive() {
    activeWatcher = buildWatcher(prefix + ACTIVE_SET, 0, watchResponse -> {
      log.debug("Saw active={}", watchResponse);

      for (WatchEvent event : watchResponse.getEvents()) {
        if (Bits.isDeleteKeyEvent(event)) {
          // IMPORTANT can't use the previous KV here since the mod revision won't reflect the
          // revision of deletion.
          final KeyValue kv = event.getKeyValue();
          handleReadyWork(WorkTransition.RELEASED, kv);
        }
      }
    });
  }

  private CompletableFuture<?> watchRegistry() {
    return etcd.getKVClient()
        .get(
            fromString(prefix + REGISTRY_SET),
            GetOption.newBuilder()
                .withPrefix(fromString(prefix + REGISTRY_SET))
                .build()
        )
        .thenAccept((getResponse) -> {
          log.debug("Initial registry response={}", getResponse);

          for (KeyValue kv : getResponse.getKvs()) {
            handleReadyWork(WorkTransition.STARTUP, kv);
          }

          registryWatcher = buildWatcher(
              prefix + REGISTRY_SET,
              getResponse.getHeader().getRevision(),
              watchResponse -> {
                log.debug("Saw registry event={}", watchResponse);

                for (WatchEvent event : watchResponse.getEvents()) {
                  if (isNewKeyEvent(event)) {
                    handleReadyWork(WorkTransition.NEW, event.getKeyValue());
                  } else if (isUpdateKeyEvent(event)) {
                    handleRegisteredWorkUpdate(event.getKeyValue());
                  } else if (isDeleteKeyEvent(event)) {
                    handleRegisteredWorkDeletion(event.getPrevKV());
                  }
                }
              }
          );
        });
  }

  private void handleRegisteredWorkUpdate(KeyValue kv) {
    final String workId = extractIdFromKey(kv);

    if (ourWork.contains(workId)) {
      log.info("Updated our work={}", workId);
      processor.update(workId, kv.getValue().toString(StandardCharsets.UTF_8));
    }
  }

  private void handleRegisteredWorkDeletion(KeyValue kv) {
    final String workId = extractIdFromKey(kv);

    if (ourWork.contains(workId)) {
      log.info("Stopping our work={}", workId);

      try {
        releaseWork(workId, kv.getValue().toString(StandardCharsets.UTF_8));
      } catch (InterruptedException e) {
        log.warn("Interrupted while releasing registered work={}", workId);
      }

    } else {
      log.info("Active work={} key was not present or not ours", workId);
      scheduleRebalance();
    }

  }

  private void watchWorkers() {
    workersWatcher = buildWatcher(prefix + WORKERS_SET, 0, watchResponse -> {
      log.debug("Saw worker={}", watchResponse);

      boolean rebalance = false;
      for (WatchEvent event : watchResponse.getEvents()) {
        if (isNewKeyEvent(event)) {
          log.info("Saw new worker={}", Bits.extractIdFromKey(event.getKeyValue()));
          rebalance = true;
        }
      }
      if (rebalance) {
        scheduleRebalance();
      }
    });
  }

  private void scheduleRebalance() {
    if (scheduledRebalance != null) {
      scheduledRebalance.cancel(false);
    }
    scheduledRebalance = taskScheduler.schedule(
        this::rebalanceWorkLoad,
        Instant.now().plus(properties.getRebalanceDelay())
    );
  }

  private CompletableFuture<?> rebalanceWorkLoad() {

    return getTargetWorkload()
        .thenAccept(targetWorkload -> {
          log.info("Rebalancing workLoad={} to target={}", workLoad.get(), targetWorkload);

          long amountToShed = workLoad.get() - targetWorkload;
          if (amountToShed > 0) {
            log.info("Shedding work to rebalance count={}", amountToShed);
            for (; amountToShed > 0; --amountToShed) {

              // give preference to shedding most recently assigned work items with the theory
              // that we'll minimize churn of long held work items
              try {
                releaseWork(null, null);
              } catch (InterruptedException e) {
                log.warn("Interrupted while releasing work");
              }
            }
          }
        });
  }

  private CompletableFuture<Long> getTargetWorkload() {
    return getCountAtPrefix(prefix + WORKERS_SET)
        .thenCompose(workersCount ->
            getCountAtPrefix(prefix + REGISTRY_SET)
                .thenApply(workCount ->
                    (long)Math.ceil((double)workCount / workersCount)
                ));
  }

  private CompletableFuture<Boolean> releaseWork(String workId, String releasedContent)
      throws InterruptedException {

    workChangeSem.acquire();
    final String workIdToRelease = (workId == null)? ourWork.peekFirst() : workId;

    // optimistic decrease
    final int newWorkLoad = workLoad.decrementAndGet();

    final ByteSequence activeKeyBytes = fromString(prefix + ACTIVE_SET + workIdToRelease);
    final ByteSequence workLoadBytes = fromFormat(WORK_LOAD_FORMAT, newWorkLoad);
    final ByteSequence ourWorkerKey = fromString(prefix + WORKERS_SET + ourId);

    log.info("Releasing work={}", workIdToRelease);

    return etcd.getKVClient().txn()
        .Then(
            // store decremented work load
            put(
                ourWorkerKey,
                workLoadBytes,
                leasedPutOption()
            ),
            // delete our active entry
            delete(
                activeKeyBytes,
                DeleteOption.DEFAULT
            )
        )
        .commit()
        .handle((txnResponse, throwable) -> {
          Boolean retval = true;
          if (throwable != null) {
            log.warn("Failure while releasing work={}", workIdToRelease, throwable);
            workLoad.incrementAndGet();
            retval = false;
          } else if (!txnResponse.isSucceeded()) {
            log.warn("Transaction failed during release of work={}", workIdToRelease);
            workLoad.incrementAndGet();
            retval = false;
          } else {
            processStoppedWork(workIdToRelease, releasedContent);
            ourWork.remove(workIdToRelease);
          }
          workChangeSem.release();
          return retval;
        });
  }

  private CompletableFuture<Long> getCountAtPrefix(String prefix) {
    final ByteSequence prefixBytes = fromString(prefix);
    return etcd.getKVClient()
        .get(
            prefixBytes,
            GetOption.newBuilder()
                .withCountOnly(true)
                .withPrefix(prefixBytes)
                .build()
        )
        .thenApply(GetResponse::getCount);
  }

  private void handleReadyWork(WorkTransition transition, KeyValue kv) {
    final String workId = Bits.extractIdFromKey(kv);
    final long revision = kv.getModRevision();

    log.info("Observed readyWork={} cause={} rev={} allocator={}",
        workId, transition, revision, ourId);

    amILeastLoaded(revision)
        .thenAccept(leastLoaded -> {
          if (leastLoaded) {
            log.info("Least loaded, so trying to grab work={}, ourId={}", workId, ourId);
            // NOTE: we can't pass the value from kv here since we might have only seen
            // an active entry deletion where all we know is workId
            try {
              grabWork(workId);
            } catch (InterruptedException e) {
              log.warn("Interrupted while grabbing work={}", workId);
            }
          }
        });
  }

  private void grabWork(String workId) throws InterruptedException {
    workChangeSem.acquire();

    // optimistically increase our workload, but we'll bump it down if txn fails
    final int newWorkLoad = workLoad.incrementAndGet();

    final ByteSequence activeKey = fromString(prefix + ACTIVE_SET + workId);
    final ByteSequence registryKey = fromString(prefix + REGISTRY_SET + workId);
    final ByteSequence ourValue = fromString(ourId);
    final ByteSequence ourWorkerKey = fromString(prefix + WORKERS_SET + ourId);
    final ByteSequence workLoadBytes = fromFormat(WORK_LOAD_FORMAT, newWorkLoad);

    etcd.getKVClient().txn()
        .If(
            // check if nobody else grabbed it
            new Cmp(activeKey, Cmp.Op.EQUAL, CmpTarget.version(0)),
            // but also check it wasn't also removed from work registry
            new Cmp(registryKey, Cmp.Op.GREATER, CmpTarget.version(0))
        )
        .Then(
            // store our active entry
            put(
                activeKey,
                ourValue,
                leasedPutOption()
            ),
            // store incremented work load
            put(
                ourWorkerKey,
                workLoadBytes,
                leasedPutOption()
            )
        )
        .commit()
        .handle((txnResponse, throwable) -> {
          log.debug("Result of grab txn = {}", txnResponse);
          Boolean retval = true;
          if (throwable != null) {
            log.warn("Failure while committing work grab of {}", workId, throwable);
            workLoad.decrementAndGet();
            retval = false;
          }

          if (txnResponse.isSucceeded()) {
            log.info("Successfully grabbed work={}, allocator={}", workId, ourId);
            ourWork.push(workId);
          } else {
            log.debug("Transaction to grab work failed {}", workId, ourId);
            workLoad.decrementAndGet();
            retval = false;
          }
          workChangeSem.release();
          return retval;
        })
        .thenCompose(success -> {
          if (success) {

            // ensure a flood of work items wasn't all picked up by us
            scheduleRebalance();

            return getWorkContent(workId)
                .thenAccept(content -> processGrabbedWork(workId, content))
                .exceptionally(throwable -> {
                  log.warn("Failed to get work={} content", workId, throwable);
                  return null;
                });
          } else {
            return CompletableFuture.completedFuture(null);
          }
        });
  }

  private PutOption leasedPutOption() {
    return PutOption.newBuilder()
        .withLeaseId(leaseId)
        .build();
  }

  private void processGrabbedWork(String workId, String content) {
    processor.start(workId, content);
  }

  private void processStoppedWork(String workId, String releasedContent) {
    if (releasedContent != null) {
      processor.stop(workId, releasedContent);
    } else {
      getWorkContent(workId)
          .thenAccept(content -> {
            processor.stop(workId, content);
          })
          .exceptionally(throwable -> {
            log.warn("Failed processing stopped work={}", workId, throwable);
            return null;
          });
    }
  }

  private CompletableFuture<String> getWorkContent(String workId) {
    return etcd.getKVClient()
        .get(
            fromString(prefix + REGISTRY_SET + workId)
        )
        .thenApply(getResponse -> getResponse.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8));
  }

  private CompletableFuture<Boolean> amILeastLoaded(long atRevision) {
    // Because of the zero-padded formatting of the work load value stored in each worker entry,
    // we can find the least loaded worker via etcd by doing an ASCII sort of the values and picking
    // off the lowest value.
    return etcd.getKVClient()
        .get(
            fromString(prefix + WORKERS_SET),
            GetOption.newBuilder()
                .withPrefix(fromString(prefix + WORKERS_SET))
                .withSortField(SortTarget.VALUE)
                .withSortOrder(SortOrder.ASCEND)
                .withLimit(1)
                .withRevision(atRevision)
                .build()
        )
        .thenApply(getResponse -> {
          if (getResponse.getCount() <= 1) {
            log.debug("Skipping least-loaded evaluation since I'm the only worker");
            // it's only us, so we're it
            return true;
          }

          // see if we're the least loaded of the current works
          final KeyValue kv = getResponse.getKvs().get(0);
          final String leastLoadedId = Bits.extractIdFromKey(kv);
          final boolean leastLoaded = ourId.equals(leastLoadedId);
          log.debug(
              "Evaluated leastLoaded={} out of workerCount={}",
              leastLoaded, getResponse.getCount()
          );
          return leastLoaded;
        });
  }

  private CompletableFuture<?> initOurWorkerEntry() {
    return etcd.getKVClient()
        .put(
            fromString(prefix + WORKERS_SET + ourId),
            Bits.fromFormat(WORK_LOAD_FORMAT, 0),
            leasedPutOption()
        );
  }

  private enum WorkTransition {
    STARTUP,
    NEW,
    RELEASED
  }

}
