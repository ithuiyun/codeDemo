package com.ithuiyun.seq;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by ithuiyun.com on 2018/4/29.
 */
public class SequenceFileDemo {
    public static void main(String[] args) throws Exception{
        //writer(args);
        reader();
    }

    /**
     * 读取sequencefile文件
     * @throws IOException
     */
    private static void reader() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop100:9000");

        Path inputPath = new Path("/hello.seq");
        Reader reader = new Reader(conf, Reader.file(inputPath));
        Text key = new Text();
        Text val = new Text();
        while (reader.next(key, val)) {
            System.out.println(key.toString());
        }
        reader.close();
    }

    /**
     * 生成sequencefile文件
     * @param args
     * @throws IOException
     * @throws URISyntaxException
     */
    private static void writer(String[] args) throws IOException, URISyntaxException {
        String inputDir = args[0];
        Path outputDir = new Path(args[1]);
        String comType = args[2];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.newInstance(new URI("hdfs://hadoop100:9000"), conf );
        fs.delete(outputDir, true);

        CompressionType compressionType = null;
        if("none".equals(comType)){
            compressionType = CompressionType.NONE;
        }else if ("block".equals(comType)){
            compressionType = CompressionType.BLOCK;
        }else if ("record".equals(comType)){
            compressionType = CompressionType.RECORD;
        }


        //构造opts数组，有4个元素，第1个是输出路径，第2个是key类型，第3个是value类型，第4个是压缩类
        Option[] opts = new Option[]{Writer.file(outputDir), Writer.keyClass(Text.class),
                Writer.valueClass(Text.class), Writer.compression(compressionType, new GzipCodec())};
        //创建一个writer实例
        Writer writer = SequenceFile.createWriter(conf, opts );
        //指定要压缩的文件的目录
        File inputDirPath = new File(inputDir);
        if(inputDirPath.isDirectory()){
            File[] files = inputDirPath.listFiles();
            for (File file : files) {
                String content = FileUtils.readFileToString(file);
                Text key = new Text(file.getName());
                Text val = new Text(content);
                writer.append(key, val);
            }
        }
        writer.close();
    }

}
