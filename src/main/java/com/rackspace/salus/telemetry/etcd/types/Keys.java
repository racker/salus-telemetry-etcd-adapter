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

import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import java.util.regex.Pattern;

public class Keys {

    public static final String DELIMITER = "/";

    public static final String FMT_ENVOYS_BY_ID = "/tenants/{tenant}/envoysById/{envoyInstanceId}";
    public static final String FMT_AGENT_INSTALLS = "/agentInstalls/{tenant}/{envoyInstanceId}/{agentType}";
    public static final Pattern PTN_AGENT_INSTALLS = Pattern.compile("/agentInstalls/(.+?)/(.+?)/(.+?)");
    public static final String FMT_AGENT_INSTALL_SELECTORS_PREFIX = "/tenants/{tenant}/agentInstallSelectors/";
    public static final String FMT_AGENT_INSTALL_SELECTORS = "/tenants/{tenant}/agentInstallSelectors/{agentType}/{agentInstallSelectorId}";
    public static final Pattern PTN_AGENT_INSTALL_SELECTORS = Pattern.compile(
        "/tenants/(?<tenant>.+?)/agentInstallSelectors/(?<agentType>.+?)/(?<agentInstallSelectorId>.+?)"
    );
    public static final String FMT_ENVOYS_BY_LABEL = "/tenants/{tenant}/envoysByLabel/{name}:{value}/{envoyInstanceId}";

    public static final String FMT_AGENTS_BY_TYPE = "/agentsByType/{agentType}/{version}/{agentId}";
    public static final String FMT_AGENTS_BY_ID = "/agentsById/{agentId}";

    public static final String FMT_ENVOYS_BY_AGENT = "/tenants/{tenant}/envoysByAgent/{agentType}/{envoyInstanceId}";
    public static final String FMT_IDENTIFIERS = "/tenants/{tenant}/identifiers/{resourceId}";
    public static final String FMT_RESOURCES_ACTIVE = "/resources/active/{md5OfTenantAndIdentifierValue}";
    public static final String FMT_WORKALLOC_REGISTRY = "/workAllocations/{realm}/registry/{partitionId}";

    /**
     * Value is count of bound monitors, leading zero padded to 10 digits
     */
    public static final String FMT_ZONE_ACTIVE = "/zones/active/{tenant}/{zoneId}/{pollerEnvoyId}";
    /**
     * Value is latest attached envoy ID
     */
    public static final String FMT_ZONE_EXPECTED = "/zones/expected/{tenant}/{zoneId}/{resourceId}";
    public static final Pattern PTN_ZONE_EXPECTED = EtcdUtils.patternFromFormat(FMT_ZONE_EXPECTED);

    public static final String PREFIX_ZONE_EXPECTED = "/zones/expected";

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
}
