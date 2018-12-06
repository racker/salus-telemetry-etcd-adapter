package com.rackspace.salus.telemetry.etcd.types;

import lombok.Data;

@Data
public class KeyRange {
  String start;
  /**
   * end is inclusive, so usage as an etcd key range-end should append '\0'
   */
  String end;
}
