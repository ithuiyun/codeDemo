package com.ithuiyun.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyProtocol extends VersionedProtocol {
    long versionID = 123456L;
    String hello(String name);
}
