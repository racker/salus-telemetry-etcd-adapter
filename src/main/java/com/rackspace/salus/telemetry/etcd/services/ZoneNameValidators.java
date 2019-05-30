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

package com.rackspace.salus.telemetry.etcd.services;

import com.rackspace.salus.telemetry.etcd.types.PrivateZoneName;
import com.rackspace.salus.telemetry.etcd.types.PublicZoneName;
import com.rackspace.salus.telemetry.etcd.types.ResolvedZone;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/**
 * Contains validators of the {@link PublicZoneName} and {@link PrivateZoneName}
 * custom constraints.
 */
public class ZoneNameValidators {

  public static class Public implements ConstraintValidator<PublicZoneName, String> {

    public void initialize(PublicZoneName constraint) { }

    public boolean isValid(String value, ConstraintValidatorContext context) {
      return value != null && value.startsWith(ResolvedZone.PUBLIC_PREFIX);
    }
  }

  public static class Private implements ConstraintValidator<PrivateZoneName, String> {

    public void initialize(PrivateZoneName constraint) { }

    public boolean isValid(String value, ConstraintValidatorContext context) {
      return value != null && !value.startsWith(ResolvedZone.PUBLIC_PREFIX);
    }

  }
}
