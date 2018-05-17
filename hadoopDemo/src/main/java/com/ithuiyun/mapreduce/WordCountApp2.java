package com.ithuiyun.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by ithuiyun.com on 2018/4/28.
 */
public class WordCountApp2 {
    /**
     * 驱动代码【组装job的代码】
     * @param args
     */
    public static void main(String[] args) throws Exception {
        //从命令行获取输入路径
        String inputPath = args[0];
        //从命令行获取输出目录
        Path outputDir = new Path(args[1]);

        //创建配置类
        Configuration conf = new Configuration();
        //表示job名称，一般建议使用类名
        String jobName = WordCountApp2.class.getSimpleName();
        //把所有的内容都封装到job中
        Job job = Job.getInstance(conf, jobName);
        //程序打jar包必备代码
        job.setJarByClass(WordCountApp2.class);


        //设置输入路径
        FileInputFormat.setInputPaths(job,inputPath);
        //设置输出路径
        FileOutputFormat.setOutputPath(job,outputDir);

        //设置自定义mapper类
        job.setMapperClass(WordCountMapper.class);
        //指定k2,v2类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置自定义reduce类型
        job.setReducerClass(WordCountReduce.class);
        //指定k3,v3类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //提交任务，等待任务执行结束
        job.waitForCompletion(true);
    }

    public static class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
        Text k2 = new Text();
        LongWritable v2 = new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\t");
            for (String word:words) {
                k2.set(word);
                v2.set(1L);
                context.write(k2,v2);
            }
        }
    }

    public static class WordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        LongWritable v3 = new LongWritable();
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context)
                throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable v2 : v2s) {
                sum += v2.get();
            }
            v3.set(sum);
            context.write(k2, v3);
        }
    }
}



