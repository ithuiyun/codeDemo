package com.ithuiyun.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class FileSystemExample {

    public static void main(String[] args)throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://huiyun100:9000");
        FileSystem fileSystem = FileSystem.get(uri, conf);
//        create(fileSystem);
//        get(fileSystem);
        ls(fileSystem);


    }

    private static void ls(FileSystem fileSystem) throws IOException {
        //列出指定目录下的文件内容
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus:fileStatuses) {
            //System.out.println(fileStatus);
            //获取文件的块的存储信息
            //其中 start 是表示从头开始 0
            // len 表示文件的长度 字节
            if (fileStatus.isFile()){
                BlockLocation[] fileBlockLocations = fileSystem.
                        getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
                //因为一个文件可能会有多个块，所以返回的是一个数组，在这里迭代这个数组
                for (BlockLocation blockLocation: fileBlockLocations) {
                    //打印文件的block块信息
                    System.out.println(blockLocation);
                }
            }
        }
    }

    private static void get(FileSystem fileSystem) throws IOException {
        //下载文件
        FSDataInputStream inputStream = fileSystem.
                open(new Path("/core-site.xml"));
        FileOutputStream fileOutputStream =
                new FileOutputStream("d:\\core-site-2.xml");
        IOUtils.copyBytes(inputStream, fileOutputStream,1024,true);
    }

    private static void create(FileSystem fileSystem) throws IOException {
        //上传文件 在这里需要在path中指定上传到hdfs中的路径名称
        FSDataOutputStream fsDataOutputStream = fileSystem.
                create(new Path("/core-site.xml"));
        //指定本地文件路径
        String localPath = "d:\\core-sute.xml";
        FileInputStream fileInputStream = new FileInputStream(localPath);
        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,1024,true);
    }

}
