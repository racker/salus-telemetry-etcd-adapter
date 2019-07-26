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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.api.DeleteRangeResponse;
import io.etcd.jetcd.api.RangeResponse;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.kv.TxnResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class contains various utility methods that simplify tedious code needed
 * for interacting with etcd.
 */
public class EtcdUtils {

    public static final int EXIT_CODE_ETCD_FAILED = 1;
    private static final Pattern KEY_PLACEHOLDER = Pattern.compile("\\{.+?\\}");

    public static ByteSequence fromString(String utf8string) {
        return ByteSequence.from(utf8string, StandardCharsets.UTF_8);
    }

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

        return fromString(sb.toString());
    }

    /**
     * This method converts a format string with <code>{...}</code> placeholders into a 
     * regex pattern with capture groups named for each format placeholder.
     * <p>
     *   For example, given the format string
     *   <pre>
     *     /zones/expected/{tenant}/{zoneName}/{resourceId}
     *   </pre>
     *   then this method will produce a regex pattern:
     *   <pre>
     *     /zones/expected/(?&lt;tenant&gt;.+?)/(?&lt;zoneName&gt;.+?)/(?&lt;resourceId&gt;.+?)
     *   </pre>
     * </p>
     * @param format a <code>{...}</code> format string
     * @return a regex pattern that can parse the given <code>format</code>
     */
    public static Pattern patternFromFormat(String format) {
        final Pattern placeholders = Pattern.compile("\\{(.+?)}");

        final Matcher matcher = placeholders.matcher(format);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, String.format("(?<%s>.+?)", matcher.group(1)));
        }
        matcher.appendTail(sb);

        return Pattern.compile(sb.toString());
    }

    public static ByteString buildByteString(String format, Object... values) {
        return ByteString.copyFromUtf8(
                String.format(format, values)
        );
    }

    public static ByteSequence buildValue(ObjectMapper objectMapper, Object o) throws JsonProcessingException {
        return ByteSequence.from(
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
                new PutResponse(io.etcd.jetcd.api.PutResponse.newBuilder()
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
            io.etcd.jetcd.api.TxnResponse.newBuilder().build()
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
                                        io.etcd.jetcd.api.KeyValue.newBuilder()
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

    /**
     * Ensures the given value is safe to use as part of a key path by doing simple encoding
     * like HTML URL encoding to replace slashes.
     * @param value the raw value
     * @return the escaped version of the given value
     */
    public static String escapePathPart(String value) {
        if (value == null) {
            return null;
        }
        return value.replace("/", "%2F");
    }

    /**
     * Reverses the processing of {@link #escapePathPart(String)}
     * @param rawValue the part of an etcd key that has been escaped by {@link #escapePathPart(String)}
     * @return the unescaped value
     */
    public static String unescapePathPart(String rawValue) {
        if (rawValue == null) {
            return rawValue;
        }
        return rawValue.replace("%2F", "/");
    }
}
