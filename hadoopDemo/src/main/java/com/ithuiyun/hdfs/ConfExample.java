package com.ithuiyun.hdfs;

import org.apache.hadoop.conf.Configuration;

public class ConfExample {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        System.out.println(conf.get("fs.defaultFS"));
    }
}
