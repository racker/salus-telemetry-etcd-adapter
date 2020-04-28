/*
 * Copyright 2020 Rackspace US, Inc.
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

package com.rackspace.salus.telemetry.etcd;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
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

        return fromString(sb.toString().toLowerCase());
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

    public static <T> T parseValue(ObjectMapper objectMapper, KeyValue kv, Class<T> type) throws IOException {
        return objectMapper.readValue(
                kv.getValue().getBytes(),
                type);
    }

    public static boolean mapContainsAll(Map<String, String> superset, Map<String, String> target) {
        return superset.entrySet().containsAll(target.entrySet());
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
