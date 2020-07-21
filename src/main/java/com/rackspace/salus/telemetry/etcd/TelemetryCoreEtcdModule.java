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

package com.rackspace.salus.telemetry.etcd;

import com.rackspace.salus.common.util.KeyHashing;
import com.rackspace.salus.telemetry.etcd.services.EtcdHealthIndicator;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

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
        .endpoints(properties.getUrl())
        .executorService(etcdExecutorService());

    if (properties.getCaCert() != null) {
      log.debug("Enabling SSL for etcd with CA cert at {}", properties.getCaCert());
      final File caFile = new File(properties.getCaCert());
      try {
        final SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient()
            .trustManager(caFile);

        if (properties.getKey() != null) {
          sslContextBuilder.
              keyManager(
                  properties.getKeyCertChain() != null ? new File(properties.getKeyCertChain())
                      : null,
                  new File(properties.getKey()),
                  properties.getKeyPassword()
              );
        }

        builder.sslContext(sslContextBuilder.build());
      } catch (SSLException e) {
        throw new IllegalStateException(
            String.format(
                "Failed to setup SSL context for etcd given CA cert at %s",
                properties.getCaCert()
            ), e);
      }
    }

    return builder.build();
  }

  private ExecutorService etcdExecutorService() {
    // A queuing executor is needed since the chained, CompletableFuture based
    // etcd operations can easily exhaust a limited thread pool and cause a deadlock
    // at the point of executor submission
    return Executors.newFixedThreadPool(properties.getMaxExecutorThreads(),
        new DefaultThreadFactory("etcd"));
  }

  @Profile("etcd-health-indicator")
  @Bean
  public EtcdHealthIndicator etcdHealthIndicator() {
    return new EtcdHealthIndicator(etcdClient());
  }
}
