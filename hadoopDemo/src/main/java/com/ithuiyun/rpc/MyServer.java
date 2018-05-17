package com.ithuiyun.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyServer {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        RPC.Builder builder = new RPC.Builder(conf);
        builder.setBindAddress("localhost")//设置监听的ip
                .setPort(1234)//设置监听的端口
                .setProtocol(MyProtocol.class)//设置rpc接口
                .setInstance(new MyProtocolImpl());//设rpc接口的实现类
        //构建server
        RPC.Server server = builder.build();
        //启动
        server.start();
        System.out.println("服务器启动了");
    }
}

