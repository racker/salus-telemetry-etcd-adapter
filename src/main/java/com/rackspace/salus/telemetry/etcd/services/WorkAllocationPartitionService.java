package com.rackspace.salus.telemetry.etcd.services;

import static com.rackspace.salus.telemetry.etcd.EtcdUtils.buildKey;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.TxnResponse;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.options.DeleteOption;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.GetOption.SortOrder;
import com.coreos.jetcd.options.GetOption.SortTarget;
import com.coreos.jetcd.options.PutOption;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.telemetry.etcd.config.KeyHashing;
import com.rackspace.salus.telemetry.etcd.types.KeyRange;
import com.rackspace.salus.telemetry.etcd.types.Keys;
import com.rackspace.salus.telemetry.etcd.types.WorkAllocationRealm;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

@Service
@Slf4j
public class WorkAllocationPartitionService {

  private final Client etcd;
  private final KeyHashing keyHashing;
  private final ObjectMapper objectMapper;
  private final IdGenerator idGenerator;

  @Autowired
  public WorkAllocationPartitionService(Client etcd, KeyHashing keyHashing,
      ObjectMapper objectMapper, IdGenerator idGenerator) {
    this.etcd = etcd;
    this.keyHashing = keyHashing;
    this.objectMapper = objectMapper;
    this.idGenerator = idGenerator;
  }

  public CompletableFuture<Boolean> changePartitions(WorkAllocationRealm realm, int count) {
    Assert.isTrue(count > 0, "partition count must be greater than zero");

    final ByteSequence prefix = buildKey(Keys.FMT_WORKALLOC_REGISTRY, realm, "");

    return etcd.getKVClient()
        .delete(
            prefix,
            DeleteOption.newBuilder()
                .withPrefix(prefix)
                .build()
        )
        .thenCompose(deleteResponse -> createPartitions(realm, count));
  }

  public CompletableFuture<List<KeyRange>> getPartitions(WorkAllocationRealm realm) {
    final ByteSequence prefix = buildKey(Keys.FMT_WORKALLOC_REGISTRY, realm, "");

    return etcd.getKVClient()
        .get(
            prefix,
            GetOption.newBuilder()
                .withPrefix(prefix)
                // the keys are UUIDs, but at least sorting will force consistent results when
                // using this operation
                .withSortField(SortTarget.KEY)
                .withSortOrder(SortOrder.ASCEND)
                .build()
        )
        .thenApply(getResponse ->
            getResponse.getKvs().stream()
                .map(keyValue -> {
                  try {
                    return objectMapper
                        .readValue(keyValue.getValue().getBytes(), KeyRange.class);
                  } catch (IOException e) {
                    log.warn("Failed to deserialize keyValue={} for realm={}",
                        keyValue.getKey().toStringUtf8(), realm, e
                    );
                    return null;
                  }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
  }

  private CompletionStage<Boolean> createPartitions(WorkAllocationRealm realm, int count) {
    final int bits = keyHashing.bits();
    final BigInteger outerValue = BigInteger.ZERO
        .setBit(bits);

    final BigInteger[] partitionChunk = outerValue
        .divideAndRemainder(BigInteger.valueOf(count));

    if (!BigInteger.ZERO.equals(partitionChunk[1])) {
      throw new IllegalArgumentException(
          String.format("Hashing bit size %d was not divisible into %d partitions",
              bits, count
          ));
    }

    BigInteger rangeStart = BigInteger.ZERO;
    final Op[] putOps = new Op[count];
    for (int i = 0; i < count; i++) {
      final BigInteger next = rangeStart.add(partitionChunk[0]);

      final KeyRange keyRange = new KeyRange()
          .setStart(format(rangeStart, bits))
          .setEnd(format(next.subtract(BigInteger.ONE), bits));

      final ByteSequence key = buildKey(
          Keys.FMT_WORKALLOC_REGISTRY, realm, idGenerator.generate());
      final ByteSequence value;
      try {
        value = ByteSequence
            .fromBytes(objectMapper.writeValueAsBytes(keyRange));
      } catch (JsonProcessingException e) {
        log.error("Failed to serialize keyRange={}", keyRange, e);
        return CompletableFuture.completedFuture(false);
      }

      putOps[i] = Op.put(key, value, PutOption.DEFAULT);

      rangeStart = next;
    }

    return etcd.getKVClient().txn().Then(putOps)
        .commit()
        .thenApply(TxnResponse::isSucceeded);
  }

  /**
   * @return the given value in hex, padded with the appropriate number of leading zeroes for the
   * overall numeric bit size
   */
  private static String format(BigInteger value, int bits) {
    return String.format("%0" + bits / 4 + "x", value);
  }

}
