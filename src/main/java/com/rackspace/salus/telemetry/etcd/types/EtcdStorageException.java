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

/**
 * Indicates that some kind of issue occurred while performing an etcd storage operation that
 * might vary from serialization issues to backend etcd issues.
 */
public class EtcdStorageException extends RuntimeException {

  public EtcdStorageException(String message) {
    super(message);
  }

  public EtcdStorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
