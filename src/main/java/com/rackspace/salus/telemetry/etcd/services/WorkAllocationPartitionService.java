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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.util.KeyHashing;
import com.rackspace.salus.telemetry.etcd.types.KeyRange;
import com.rackspace.salus.telemetry.etcd.workpart.WorkAllocator;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

@Slf4j
public class WorkAllocationPartitionService {

  private final WorkAllocator workAllocator;
  private final KeyHashing keyHashing;
  private final ObjectMapper objectMapper;

  @Autowired
  public WorkAllocationPartitionService(WorkAllocator workAllocator, KeyHashing keyHashing,
                                        ObjectMapper objectMapper) {
    this.workAllocator = workAllocator;
    this.keyHashing = keyHashing;
    this.objectMapper = objectMapper;
  }

  public CompletableFuture<Boolean> changePartitions(int count) {
    Assert.isTrue(count > 0, "partition count must be greater than zero");

    try {
      final List<String> workContents = createPartitions(count);

      return workAllocator.bulkReplaceWork(workContents);

    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to encode work partition content", e);
    }
  }

  public CompletableFuture<List<KeyRange>> getPartitions() {
    return workAllocator.getWorkRegistry()
        .thenApply(workEntries ->
            workEntries.stream()
                .map(work -> {
                  try {
                    return objectMapper.readValue(work.getContent(), KeyRange.class);
                  } catch (IOException e) {
                    log.warn(
                        "Unable to parse work partition key range from content={}",
                        work.getContent(), e
                    );
                    throw new IllegalStateException("Unable parse work partition key range", e);
                  }
                })
                .collect(Collectors.toList()));
  }

  private List<String> createPartitions(int count)
      throws JsonProcessingException {
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

    final List<String> workContents = new ArrayList<>(count);

    BigInteger rangeStart = BigInteger.ZERO;
    for (int i = 0; i < count; i++) {
      final BigInteger next = rangeStart.add(partitionChunk[0]);

      final KeyRange keyRange = new KeyRange()
          .setStart(format(rangeStart, bits))
          .setEnd(format(next.subtract(BigInteger.ONE), bits));

      workContents.add(
          objectMapper.writeValueAsString(keyRange)
      );

      rangeStart = next;
    }

    return workContents;
  }

  /**
   * @return the given value in hex, padded with the appropriate number of leading zeroes for the
   * overall numeric bit size
   */
  private static String format(BigInteger value, int bits) {
    return String.format("%0" + bits / 4 + "x", value);
  }

}
