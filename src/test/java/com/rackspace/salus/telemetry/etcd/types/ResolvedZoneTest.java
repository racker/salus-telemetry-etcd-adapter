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

import static com.rackspace.salus.telemetry.etcd.types.ResolvedZone.createPrivateZone;
import static com.rackspace.salus.telemetry.etcd.types.ResolvedZone.createPublicZone;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import org.junit.Test;

public class ResolvedZoneTest {

  @Test
  public void getTenantForKey_nonPublic() {
    final ResolvedZone zone = createPrivateZone("tenant-1", "z-1");

    assertThat(zone.getTenantForKey(), equalTo("tenant-1"));
  }

  @Test
  public void getTenantForKey_public() {
    final ResolvedZone zone = createPublicZone("z-1");

    assertThat(zone.getTenantForKey(), equalTo(ResolvedZone.PUBLIC));
  }

  @Test
  public void getTenantAlwaysNullForPublic() {
    final ResolvedZone zone = createPublicZone("z-1");

    assertThat(zone.getTenantId(), nullValue());
  }

  @Test
  public void getZoneNameForKey() {
    final ResolvedZone zone = createPublicZone("companyName/west");

    assertThat(zone.getZoneNameForKey(), equalTo("companyname%2Fwest"));
  }

  @Test
  public void testFromKeyParts_public() {
    final ResolvedZone resolvedZone = ResolvedZone
        .fromKeyParts(ResolvedZone.PUBLIC, EtcdUtils.escapePathPart("public/west"));

    assertThat(resolvedZone, notNullValue());
    assertThat(resolvedZone.isPublicZone(), equalTo(true));
    assertThat(resolvedZone.getName(), equalTo("public/west"));
    assertThat(resolvedZone.getTenantId(), nullValue());
  }

  @Test
  public void testFromKeyParts_private() {
    final ResolvedZone resolvedZone = ResolvedZone
        .fromKeyParts("t-1", EtcdUtils.escapePathPart("custom/division1"));

    assertThat(resolvedZone, notNullValue());
    assertThat(resolvedZone.isPublicZone(), equalTo(false));
    assertThat(resolvedZone.getName(), equalTo("custom/division1"));
    assertThat(resolvedZone.getTenantId(), equalTo("t-1"));
  }
}