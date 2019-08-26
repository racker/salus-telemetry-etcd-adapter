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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.util.KeyHashing;
import com.rackspace.salus.telemetry.etcd.types.KeyRange;
import com.rackspace.salus.telemetry.etcd.workpart.WorkAllocator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WorkAllocationPartitionServiceTest {

  @Mock
  WorkAllocator workAllocator;

  private ObjectMapper objectMapper = new ObjectMapper();

  private WorkAllocationPartitionService service;

  @Before
  public void setUp() {
    service = new WorkAllocationPartitionService(workAllocator,
        new KeyHashing(), objectMapper
    );
  }

  private String encodeKeyRange(String start, String end) throws JsonProcessingException {
    return objectMapper.writeValueAsString(new KeyRange().setStart(start).setEnd(end));
  }

  @Test
  public void testChangePartitions()
      throws ExecutionException, InterruptedException, JsonProcessingException {

    when(workAllocator.bulkReplaceWork(any()))
        .thenReturn(CompletableFuture.completedFuture(true));

    final Boolean result = service.changePartitions(2)
        .get();

    assertTrue(result);

    List<String> expectedWorkContents = List.of(
        encodeKeyRange(
            "0000000000000000000000000000000000000000000000000000000000000000",
            "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        ),
        encodeKeyRange(
            "8000000000000000000000000000000000000000000000000000000000000000",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        )
    );
    verify(workAllocator).bulkReplaceWork(expectedWorkContents);
  }

  @Test
  public void testChangePartitions_invalidCount() {

    assertThatThrownBy(() -> service.changePartitions(3)
        .get())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Hashing bit size 256 was not divisible into 3 partitions");

    verify(workAllocator, never()).bulkReplaceWork(anyList());
  }
}