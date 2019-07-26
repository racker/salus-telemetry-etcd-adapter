/*
 *    Copyright 2018 Rackspace US, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 *
 */

package com.rackspace.salus.telemetry.etcd.types;

import io.etcd.jetcd.ByteSequence;
import lombok.Data;

/**
 * This class is useful for streamed operators where the key of the object needs to be
 * propagated to enable further operations on that key.
 *
 * <p>
 *     Here's an example of where this can be used:
 * </p>
 * <pre>
 getResponse.getKvs().stream()
 .map(kv -> {
     try {
         return KeyedValue.of(
             kv.getKey(),
             parseValue(objectMapper, kv, AgentInstallSelector.class)
         );
     } catch (IOException e) {
         log.warn("Unable to parse AgentInstallSelector from {}",
         kv.getValue().toString(StandardCharsets.UTF_8), e);
         return null;
     }
 })
 .filter(Objects::nonNull)
 .filter(existing ->
     existing.getValue().getLabels().equals(agentInstallSelector.getLabels()))
 .map(existing ->
     etcd.getKVClient().delete(existing.getKey()))

 * </pre>
 * @param <T> the type of value you want this to hold
 */
@Data
public class KeyedValue<T> {
    final ByteSequence key;
    final T value;

    public static <T> KeyedValue<T> of(ByteSequence key, T value) {
        return new KeyedValue<>(key, value);
    }
}
