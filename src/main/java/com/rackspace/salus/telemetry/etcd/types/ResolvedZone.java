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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * This class is both a data holder and utility for resolving given zone IDs and optionally
 * owning tenant IDs. It keeps track of the distinction between public zones, which are not
 * owned by any one tentant, and private zones, which are tenant owned. The utility portion of
 * the class assists with etcd key-path conversion where the slashes in zone identifiers need
 * to be converted to avoid conflicting with the slash-delimited convention of the etcd key
 * paths.
 */
@Getter @ToString @EqualsAndHashCode
public class ResolvedZone {

  public static final String PUBLIC = "_PUBLIC_";

  final String id;
  final String tenantId;

  private ResolvedZone(String zoneId) {
    this.tenantId = null; // indicates public
    this.id = zoneId;
  }

  private ResolvedZone(String zoneTenantId, String zoneId) {
    this.tenantId = zoneTenantId;
    this.id = zoneId;
  }

  public static ResolvedZone createPublicZone(String zoneId) {
    return new ResolvedZone(zoneId);
  }

  public static ResolvedZone createPrivateZone(String zoneTenantId, String zoneId) {
    return new ResolvedZone(zoneTenantId, zoneId);
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

    final String correctedZoneId = EtcdUtils.unescapePathPart(zone);
    if (!tenant.equals(PUBLIC)) {
      return createPrivateZone(tenant, correctedZoneId);
    }
    else {
      return createPublicZone(correctedZoneId);
    }
  }
}
