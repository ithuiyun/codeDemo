package com.ithuiyun.rpc;

import org.apache.hadoop.ipc.ProtocolSignature;
import java.io.IOException;

public class MyProtocolImpl implements MyProtocol {

    @Override
    public String hello(String name) {
        System.out.println("我被调用了");
        return "hello "+name;
    }
    //接口版本
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return versionID;
    }
    //协议签名
    @Override
    public ProtocolSignature getProtocolSignature(
            String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        return new ProtocolSignature();
    }
}
