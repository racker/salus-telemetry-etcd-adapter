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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.rackspace.salus.telemetry.model.AgentType;
import io.etcd.jetcd.ByteSequence;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.Test;

public class EtcdUtilsTest {

    @Test
    public void buildKey_typical() {
        final ByteSequence result = EtcdUtils.buildKey(
            "/tenants/{tenant}/agentInstallSelectors/{agentType}/{agentInstallSelectorId}",
            "t1", AgentType.FILEBEAT, "ais1"
        );

        assertEquals("/tenants/t1/agentInstallSelectors/FILEBEAT/ais1", result.toString(
            StandardCharsets.UTF_8));
    }

    @Test
    public void buildKey_noVars() {
        final ByteSequence result = EtcdUtils.buildKey("/agentInstalls");

        assertEquals("/agentInstalls", result.toString(StandardCharsets.UTF_8));
    }

    @Test(expected = IllegalArgumentException.class)
    public void buildKey_tooManyValues() {
        EtcdUtils.buildKey(
            "/tenants/{tenant}/agentInstallSelectors",
            "t1", AgentType.FILEBEAT, "ais1"
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void buildKey_notEnoughValues() {
        EtcdUtils.buildKey(
            "/tenants/{tenant}/agentInstallSelectors/{agentType}/{agentInstallSelectorId}",
            "t1"
        );
    }

    @Test
    public void testContainsAll_typical() {
        final Map<String, String> superset = new HashMap<>();
        superset.put("os", "LINUX");
        superset.put("arch", "X86_64");

        final Map<String, String> target = new HashMap<>();
        target.put("os", "LINUX");

        final boolean result = EtcdUtils.mapContainsAll(superset, target);

        assertTrue(result);
    }

    @Test
    public void testContainsAll_noMatch() {
        final Map<String, String> superset = new HashMap<>();
        superset.put("os", "WINDOWS");

        final Map<String, String> target = new HashMap<>();
        target.put("os", "LINUX");

        final boolean result = EtcdUtils.mapContainsAll(superset, target);

        assertFalse(result);
    }

    @Test
    public void testContainsAll_differentKeys() {
        final Map<String, String> superset = new HashMap<>();
        superset.put("arch", "X86");

        final Map<String, String> target = new HashMap<>();
        target.put("os", "LINUX");

        final boolean result = EtcdUtils.mapContainsAll(superset, target);

        assertFalse(result);
    }

    @Test
    public void testContainsAll_exact() {
        final Map<String, String> superset = new HashMap<>();
        superset.put("os", "LINUX");

        final Map<String, String> target = new HashMap<>();
        target.put("os", "LINUX");

        final boolean result = EtcdUtils.mapContainsAll(superset, target);

        assertTrue(result);
    }

    @Test
    public void testEscapePathPart_withSlash() {
        final String result = EtcdUtils.escapePathPart("one/two");

        assertEquals("one%2Ftwo", result);
    }

    @Test
    public void testEscapePathPart_withMultipleSlashes() {
        final String result = EtcdUtils.escapePathPart("one/two/three");

        assertEquals("one%2Ftwo%2Fthree", result);
    }

    @Test
    public void testEscapePathPart_withoutSlash() {
        final String result = EtcdUtils.escapePathPart("one-two");

        assertEquals("one-two", result);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testEscapePathPart_null() {
        final String result = EtcdUtils.escapePathPart(null);

        assertNull(result);
    }

    @Test
    public void testUnescapePathPart_null() {
        final String result = EtcdUtils.unescapePathPart(null);

        assertNull(result);
    }

    @Test
    public void testUnescapePathPart_roundTrip() {
        final String in = EtcdUtils.escapePathPart("public/west");
        final String result = EtcdUtils.unescapePathPart(in);

        assertEquals("public/west", result);
    }

    @Test
    public void testUnescapePathPart_withEscapes() {
        final String result = EtcdUtils.unescapePathPart("public%2Fwest");

        assertEquals("public/west", result);
    }

    @Test
    public void testUnescapePathPart_noEscapes() {
        final String result = EtcdUtils.unescapePathPart("simple");

        assertEquals("simple", result);
    }

    @Test
    public void testPatternFromFormat_fields() {
        final Pattern result = EtcdUtils
            .patternFromFormat("/zones/expected/{tenant}/{zoneName}/{resourceId}");

        assertEquals("/zones/expected/(?<tenant>.+?)/(?<zoneName>.+?)/(?<resourceId>.+?)", result.toString());
    }

    @Test
    public void testPatternFromFormat_noFields() {
        final Pattern result = EtcdUtils
            .patternFromFormat("/zones/expected");

        assertEquals("/zones/expected", result.toString());
    }
}