package com.ithuiyun.grouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 *
 * Created by ithuiyun.com on 2018/4/28.
 */
public class GroupingWordCountApp1 {
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
        String jobName = GroupingWordCountApp1.class.getSimpleName();
        //把所有的内容都封装到job中
        Job job = Job.getInstance(conf, jobName);
        //程序打jar包必备代码
        job.setJarByClass(GroupingWordCountApp1.class);



        //设置输入路径
        FileInputFormat.setInputPaths(job,inputPath);
        //设置输出路径
        FileOutputFormat.setOutputPath(job,outputDir);

        //设置自定义mapper类
        job.setMapperClass(SortMapper.class);
        //指定k2,v2类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置自定义reduce类型
        job.setReducerClass(SortReduce.class);
        //指定k3,v3类型
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        //提交任务，等待任务执行结束
        job.waitForCompletion(true);
    }

    public static class SortMapper extends Mapper<LongWritable,Text,LongWritable,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            context.write(new LongWritable(Long.parseLong(split[0])),new LongWritable(Long.parseLong(split[1])));
        }
    }

    public static class SortReduce extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>{
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long min = Long.MAX_VALUE;
            // 遍历比较求出每个组中的最小值
            for (LongWritable val : values) {
                if (val.get() < min) {
                    min = val.get();
                }
            }
            context.write(key, new LongWritable(min));
        }
    }



}



