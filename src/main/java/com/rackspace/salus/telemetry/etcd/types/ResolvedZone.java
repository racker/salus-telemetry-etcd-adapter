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

package com.rackspace.salus.telemetry.etcd.types;

import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import lombok.Data;

@Data
public class ResolvedZone {

  public static final String PUBLIC = "_PUBLIC_";

  String id;
  String tenantId;

  public ResolvedZone setPublicZone(boolean value) {
    if (value) {
      this.tenantId = null;
    }
    return this;
  }

  public boolean isPublicZone() {
    return this.tenantId == null;
  }

  public String getTenantForKey() {
    if (isPublicZone()) {
      return PUBLIC;
    }
    else {
      return tenantId;
    }
  }

  public String getZoneIdForKey() {
    return EtcdUtils.escapePathPart(id);
  }

  public static ResolvedZone fromKeyParts(String tenant, String zone) {

    final ResolvedZone resolvedZone = new ResolvedZone()
        .setId(EtcdUtils.unescapePathPart(zone));

    if (!tenant.equals(PUBLIC)) {
      resolvedZone
          .setTenantId(tenant)
          .setPublicZone(false);
    }
    else {
      resolvedZone
          .setPublicZone(true);
    }

    return resolvedZone;
  }
}
