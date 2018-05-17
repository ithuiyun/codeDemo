package com.ithuiyun.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import java.net.InetSocketAddress;

public class MyClient {
    public static void main(String[] args) throws Exception {
        InetSocketAddress addr = new InetSocketAddress(
                "localhost",1234);
        Configuration conf = new Configuration();

        MyProtocol proxy = RPC.getProxy(
                MyProtocol.class, MyProtocol.versionID, addr, conf);
        String result = proxy.hello("徽云");
        System.out.println("客户端收到的结果："+result);
    }
}
