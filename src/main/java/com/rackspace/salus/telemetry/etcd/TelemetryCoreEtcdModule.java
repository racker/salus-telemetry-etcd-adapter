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

package com.rackspace.salus.telemetry.etcd;

import com.rackspace.salus.common.util.KeyHashing;
import com.rackspace.salus.telemetry.etcd.services.EtcdHealthIndicator;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.grpc.netty.GrpcSslContexts;
import java.io.File;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

@SpringBootConfiguration
@ComponentScan
@Import(KeyHashing.class)
@Slf4j
public class TelemetryCoreEtcdModule {

  private final EtcdProperties properties;

  @Autowired
  public TelemetryCoreEtcdModule(EtcdProperties etcdProperties) {
    this.properties = etcdProperties;
  }

  @Bean
  public Client etcdClient() {
    log.debug("Configuring etcd connectivity to {}", properties.getUrl());
    final ClientBuilder builder = Client.builder()
        .endpoints(properties.getUrl());

    if (properties.getCaCert() != null) {
      log.debug("Enabling SSL for etcd with CA cert at {}", properties.getCaCert());
      final File caFile = new File(properties.getCaCert());
      try {
        builder.sslContext(GrpcSslContexts.forClient()
            .trustManager(caFile)
            .build()
        );
      } catch (SSLException e) {
        throw new IllegalStateException(
            String.format("Failed to setup SSL context for etcd given CA cert at %s", properties.getCaCert()), e);
      }
    }

    return builder.build();
  }

  @Bean
  public EtcdHealthIndicator etcdHealthIndicator() {
    return new EtcdHealthIndicator(etcdClient());
  }
}
