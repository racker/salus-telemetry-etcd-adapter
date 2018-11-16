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

package com.rackspace.salus.telemetry.etcd;

import com.coreos.jetcd.api.DeleteRangeResponse;
import com.coreos.jetcd.api.RangeResponse;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.DeleteResponse;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.kv.PutResponse;
import com.coreos.jetcd.kv.TxnResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EtcdUtils {

    private static final Pattern KEY_PLACEHOLDER = Pattern.compile("\\{.+?\\}");

    /**
     * Replaces placeholders in the <code>format</code> string with the <code>values</code>
     * in the same position. A placeholder syntax is <code>{name}</code>; however, the name itself
     * is actually ignored. The name is only there for readability when looking at a call to this method.
     *
     * <p>
     *     For example:
     * </p>
     * <pre>
 final ByteSequence result = EtcdUtils.buildKey(
     "/tenants/{tenant}/agentInstallSelectors/{agentType}/{agentInstallSelectorId}",
     "t1", AgentType.FILEBEAT, "ais1"
 );
     * </pre>
     * @param format a format string with <code>{name}</code> placeholders
     * @param values the values to place into the placeholders
     * @return a {@link ByteSequence} of the string after placeholder replacement
     */
    public static ByteSequence buildKey(String format, Object... values) {

        final Matcher matcher = KEY_PLACEHOLDER.matcher(format);
        int i = 0;
        final StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            if (i >= values.length) {
                throw new IllegalArgumentException(String.format("Not enough values provided for %s", matcher.group()));
            }
            matcher.appendReplacement(sb, values[i++].toString());
        }
        if (i < values.length) {
            throw new IllegalArgumentException("Too many values were provided");
        }
        matcher.appendTail(sb);

        return ByteSequence.fromString(sb.toString());
    }

    public static ByteString buildByteString(String format, Object... values) {
        return ByteString.copyFromUtf8(
                String.format(format, values)
        );
    }

    public static ByteSequence buildValue(ObjectMapper objectMapper, Object o) throws JsonProcessingException {
        return ByteSequence.fromBytes(
                objectMapper.writeValueAsBytes(o)
        );
    }

    public static <T> T parseValue(ObjectMapper objectMapper, KeyValue kv, Class<T> type) throws IOException {
        return objectMapper.readValue(
                kv.getValue().getBytes(),
                type);
    }

    /**
     * Builds an empty and completed PutResponse for use in collection-to-completion-chains
     * @return completed PutResponse
     */
    public static CompletableFuture<PutResponse> completedPutResponse() {
        return CompletableFuture.completedFuture(
                new PutResponse(com.coreos.jetcd.api.PutResponse.newBuilder()
                        .build())
        );
    }

    /**
     * Builds an empty and completed DeleteResponse for use in collection-to-completion-chains
     * @return completed DeleteResponse
     */
    public static CompletableFuture<DeleteResponse> completedDeletedResponse() {
        return CompletableFuture.completedFuture(new DeleteResponse(
                DeleteRangeResponse.newBuilder().build()
        ));
    }

    public static CompletableFuture<TxnResponse> completedTxnResponse() {
        return CompletableFuture.completedFuture(new TxnResponse(
            com.coreos.jetcd.api.TxnResponse.newBuilder().build()
        ));
    }

    /**
     * This is intended to be used in a {@link java.util.stream.Stream#reduce(BinaryOperator)} to take a stream
     * of {@link CompletableFuture}s, wait for all them to be resolved, and in turn reduce the stream to a single,
     * pending CompletableFuture.
     *
     * <p>
     *     An example usage is:
     *     <pre>
 .map(existing -> {
     return etcd.getKVClient().delete(existing.getKey());
 })
 .reduce(EtcdUtils::byComposingCompletables)
 .orElse(completedDeletedResponse())
 .thenApply(deleteResponse -> agentType);
     *     </pre>
     * </p>
     * @return a {@link CompletableFuture} that is resolved when the given completables are resolved
     */
    public static <T> CompletableFuture<T> byComposingCompletables(CompletableFuture<T> completableL,
                                                                   CompletableFuture<T> completableR) {
        return completableL.thenCompose(o -> completableR);
    }

    public static boolean mapContainsAll(Map<String, String> superset, Map<String, String> target) {
        return superset.entrySet().containsAll(target.entrySet());
    }

    public static CompletableFuture<GetResponse> buildGetResponse(ObjectMapper objectMapper,
                                                                  String key, Object value) throws JsonProcessingException {
        return CompletableFuture.completedFuture(
                new GetResponse(
                        RangeResponse.newBuilder()
                                .addKvs(
                                        com.coreos.jetcd.api.KeyValue.newBuilder()
                                                .setKey(ByteString.copyFromUtf8(key))
                                                .setValue(ByteString.copyFrom(
                                                        objectMapper.writeValueAsBytes(value)
                                                ))
                                                .build()
                                )
                                .setCount(1)
                                .build()
                )
        );
    }


}
