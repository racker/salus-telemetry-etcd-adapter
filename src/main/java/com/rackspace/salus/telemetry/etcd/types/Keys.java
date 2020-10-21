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

package com.rackspace.salus.telemetry.etcd.types;

import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import java.util.regex.Pattern;

public class Keys {

    public static final String FMT_RESOURCE_ACTIVE_BY_RESOURCE_ID = "/tenants/{tenant}/identifiers/{resourceId}";
    public static final String FMT_RESOURCE_ACTIVE_BY_HASH = "/resources/active/{md5OfTenantAndIdentifierValue}";

    /**
     * Value is not used, only the key existence is significant
     */
    public static final String FMT_ZONE_ACTIVE = "/zones/active/{tenant}/{zoneName}/{resourceId}";
    public static final Pattern PTN_ZONE_ACTIVE = EtcdUtils.patternFromFormat(FMT_ZONE_ACTIVE);
    /**
     * Value is latest attached envoy ID
     */
    public static final String FMT_ZONE_EXPECTED = "/zones/expected/{tenant}/{zoneName}/{resourceId}";
    public static final Pattern PTN_ZONE_EXPECTED = EtcdUtils.patternFromFormat(FMT_ZONE_EXPECTED);
    /**
     * Value is latest attached envoy ID
     */
    public static final String FMT_ZONE_EXPIRING = "/zones/expiring/{tenant}/{zoneName}/{resourceId}";
    public static final Pattern PTN_ZONE_EXPIRING = EtcdUtils.patternFromFormat(FMT_ZONE_EXPIRING);
    public static final String FMT_ZONE_EXPIRING_IN_ZONE = "/zones/expiring/{tenant}/{zoneName}/";

    public static final String PREFIX_ZONE_EXPECTED = "/zones/expected";
    public static final String PREFIX_ZONE_ACTIVE = "/zones/active";
    public static final String PREFIX_ZONE_EXPIRING = "/zones/expiring";

    /**
     * This key is only used to store the revision version that was last read by {@link com.rackspace.salus.telemetry.etcd.services.ZoneStorage}.
     * The revision relates to the watch events seen with the PTN_ZONE_EXPECTED key prefix.
     *
     * If the watcher restarts it can start listening from the latest watch event
     * processed in PTN_ZONE_EXPECTED vs. reading from the start and processing irrelevant events.
     *
     * Currently only one process is reading/writing these keys so we will not have a case where
     * it has to read through a backlog of events after starting up.
     */
    public static final String TRACKING_KEY_ZONE_EXPECTED = "/tracking/zones/expected";
    public static final String TRACKING_KEY_ZONE_ACTIVE = "/tracking/zones/active";
    public static final String TRACKING_KEY_ZONE_EXPIRING = "/tracking/zones/expiring";
}
