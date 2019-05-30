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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import com.rackspace.salus.telemetry.etcd.types.PrivateZoneName;
import com.rackspace.salus.telemetry.etcd.types.PublicZoneName;
import java.util.Set;
import javax.validation.ConstraintViolation;
import org.junit.Before;
import org.junit.Test;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

public class ZoneNameValidatorsTest {
  private LocalValidatorFactoryBean validatorFactoryBean;

  @Before
  public void setUp() {
    validatorFactoryBean = new LocalValidatorFactoryBean();
    validatorFactoryBean.afterPropertiesSet();
  }

  @Test
  public void testPublicZoneName_valid() {
    final WithPublicZoneName obj = new WithPublicZoneName();
    obj.zoneName = "public/west";

    final Set<ConstraintViolation<WithPublicZoneName>> results = validatorFactoryBean
        .validate(obj);

    assertThat(results, hasSize(0));
  }

  @Test
  public void testPublicZoneName_invalid() {
    final WithPublicZoneName obj = new WithPublicZoneName();
    obj.zoneName = "notPublic";

    final Set<ConstraintViolation<WithPublicZoneName>> results = validatorFactoryBean
        .validate(obj);

    assertThat(results, hasSize(1));
    assertThat(
        results.iterator().next().getConstraintDescriptor().getAnnotation().annotationType(),
        equalTo(PublicZoneName.class)
    );
  }

  @Test
  public void testPrivateZoneName_valid() {
    final WithPrivateZoneName obj = new WithPrivateZoneName();
    obj.zoneName = "dev";

    final Set<ConstraintViolation<WithPrivateZoneName>> results = validatorFactoryBean
        .validate(obj);

    assertThat(results, hasSize(0));
  }

  @Test
  public void testPrivateZoneName_invalid() {
    final WithPrivateZoneName obj = new WithPrivateZoneName();
    obj.zoneName = "public/any";

    final Set<ConstraintViolation<WithPrivateZoneName>> results = validatorFactoryBean
        .validate(obj);

    assertThat(results, hasSize(1));
    assertThat(
        results.iterator().next().getConstraintDescriptor().getAnnotation().annotationType(),
        equalTo(PrivateZoneName.class)
    );
  }

  static class WithPublicZoneName {
    @PublicZoneName
    String zoneName;
  }

  static class WithPrivateZoneName {
    @PrivateZoneName
    String zoneName;
  }
}