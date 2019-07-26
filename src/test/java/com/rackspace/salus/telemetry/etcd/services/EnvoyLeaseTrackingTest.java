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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rackspace.salus.telemetry.etcd.EtcdProperties;
import com.rackspace.salus.telemetry.etcd.EtcdUtils;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.api.KeyValue;
import io.etcd.jetcd.api.RangeResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@JsonTest
public class EnvoyLeaseTrackingTest {
    // Disable full component scan and just import what we're testing
    @Configuration
    @Import({EnvoyLeaseTracking.class, EtcdProperties.class})
    public static class TestConfig {

    }

    @MockBean
    Client etcd;

    @Mock
    KV kv;

    @Mock
    Lease lease;

    @Mock
    LeaseGrantResponse grantResponse;

    @Mock
    LeaseKeepAliveResponse keepAliveResponse;

    @Mock LeaseRevokeResponse leaseRevokeResponse;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    EnvoyLeaseTracking envoyLeaseTracking;

    @Before
    public void setUp() throws Exception {
        when(etcd.getKVClient()).thenReturn(kv);
    }

    @Test
    public void testRetrieve() {
        when(kv.get(EtcdUtils.fromString("/tenants/t1/envoysById/e1")))
            .thenReturn(
                CompletableFuture.completedFuture(
                    new GetResponse(RangeResponse.newBuilder()
                        .setCount(1)
                        .addKvs(KeyValue.newBuilder()
                            .setLease(2000)
                            .build())
                        .build())
                )
            );

        final Long result = envoyLeaseTracking.retrieve("t1", "e1").join();

        assertEquals(Long.valueOf(2000), result);
    }
    @Test
    public void testLeases() {
        @SuppressWarnings("WrapperTypeMayBePrimitive")
        Long leaseId = 50L;
        String envoyInstance = "t1";
        
        when(etcd.getLeaseClient()).thenReturn(lease);
        when(lease.grant(anyLong())).thenReturn(CompletableFuture.completedFuture(grantResponse));
        when(grantResponse.getID()).thenReturn(leaseId);
        final Long result = envoyLeaseTracking.grant(envoyInstance).join();

        assertEquals(leaseId, result);

        when(lease.keepAliveOnce(leaseId)).thenReturn(CompletableFuture.completedFuture(keepAliveResponse));
        envoyLeaseTracking.keepAlive(envoyInstance);
        verify(lease).keepAliveOnce(leaseId);

        when(lease.revoke(leaseId)).thenReturn(CompletableFuture.completedFuture(leaseRevokeResponse));
        envoyLeaseTracking.revoke(envoyInstance);
        verify(lease).revoke(leaseId);
    }
}
