## User Stories

An example of a user story that drove the key design below:

> As a system admin, I want to declare a new version of filebeat for device owners to install

> As a device owner, I want filebeat v6.3.2 running on all my os=linux nodes

> As a device owner, I want to gather the `/var/log/messages` logs from all my os=linux nodes using the filebeat agent

## Organizational Patterns

* System, such as agent version/download definitions (AgentInfo)
* Tenant-persistent, such as agent install selectors
* Tenant-envoy-transient, such as envoy registration, agentInstalls, and appliedConfigs

## Keys

```
/tenants/{tenant}/envoysById/{envoyInstanceId} = EnvoySummary
                             ^
                             ^ - leased to envoyInstanceId

/tenants/{tenant}/envoysByLabel/{name}:{value}/{envoyInstanceId} = envoyInstanceId
                                               ^
                                               ^ - leased to envoyInstanceId

# Used to determine which agent installs and config assignments are applicable in conjunction with label selectors
/tenants/{tenant}/envoysByAgent/{agentType}/{envoyInstanceId} = envoyInstanceId
                                            ^
                                            ^ - leased to envoyInstanceId

/tenants/{tenant}/agentInstallSelectors/{agentType}/{agentInstallSelectorId} = AgentInstallSelector

/agentInstalls/{tenant}/{envoyInstanceId}/{agentType} = agentInfoId
^                       ^
^                       ^ - leased to envoyInstanceId after ambassador observes first-put
^
^ - each ambassador watches here, but only acts upon locally tracked envoyInstanceId's

# FMT_AGENT_CONFIGS
/tenants/{tenant}/agentConfigs/{agentType}/{agentConfigId} = AgentConfig

/appliedConfigs/{selectorScope}/{tenant}/{agentConfigId}/{envoyInstanceId} = AppliedConfig
^                                                              ^
^                                                              ^ - leased to envoyInstanceId after ambassador observes first-put
^
^ - each ambassador watches here, but only acts upon locally tracked envoyInstanceId's

# Eventually these two might make more sense to be stored in a RDBMS
/agentsByType/{agentType}/{version}/{agentId} = AgentInfo
/agentsById/{agentId} = AgentInfo
```

## Data Types

```
agentType:
  enum of
    telegraf
    filebeat

AgentInfo:
  id
  version
  type (filebeat, telegraf)
  os
  arch
  url
  checksum

AgentInstallSelector:
  agentInfoId
  labels[]
    name
    value
```

