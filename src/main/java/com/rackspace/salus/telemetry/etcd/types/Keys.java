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

import java.util.regex.Pattern;

public class Keys {
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
    public static final String FMT_AGENT_CONFIGS_PREFIX = "/tenants/{tenant}/agentConfigs";
    public static final String FMT_AGENT_CONFIGS = FMT_AGENT_CONFIGS_PREFIX + "/byId/{agentConfigId}";
    public static final String FMT_APPLIED_CONFIGS_PREFIX = "/appliedConfigs";
    public static final String FMT_APPLIED_CONFIGS = FMT_APPLIED_CONFIGS_PREFIX +
        "/{selectorScope}/{tenant}/{agentConfigId}/{envoyInstanceId}";
    public static final Pattern PTN_APPLIED_CONFIGS = Pattern.compile(
        "/appliedConfigs/(?<scope>.+?)/(?<tenant>.+?)/(?<agentConfigId>.+?)/(?<envoyInstanceId>.+?)");
    public static final String FMT_IDENTIFIERS = "/tenants/{tenant}/identifiers/{resourceId}";
    public static final String FMT_IDENTIFIERS_BY_TENANT = "/tenants/{tenant}/identifiers";
    public static final String FMT_IDENTIFIERS_BY_IDENTIFIER = "/tenants/{tenant}/identifiers/{identifierName}";
    public static final String FMT_RESOURCES_ACTIVE = "/resources/active/{md5OfTenantAndIdentifierValue}";
    public static final String FMT_RESOURCES_EXPECTED = "/resources/expected/{md5OfTenantAndIdentifierValue}";
    public static final String FMT_WORKALLOC_REGISTRY = "/workAllocations/{realm}/registry/{partitionId}";
}
