package com.ithuiyun.mapreduce;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ithuiyun.com on 2018/4/29.
 */
public class AnchorVideoInfo3 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //处理命令行传递的参数
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //注意：此时这两个参数不能直接从main函数的args参数中获取，需要从remainingArgs中获取
        String inputPath = remainingArgs[0];
        String outPutPath = remainingArgs[1];



        String jobName = AnchorVideoInfo3.class.getSimpleName();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(AnchorVideoInfo3.class);

        //设置输入 输出
        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,new Path(outPutPath));


        //设置mapper 和 reducer
        job.setMapperClass(AnchorVideoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VideoInfoWritable.class);

        job.setReducerClass(AnchorVideoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VideoInfoWritable.class);

        //提交运行
        job.waitForCompletion(true);

    }



    public static class AnchorVideoMapper extends Mapper<LongWritable,Text,Text,VideoInfoWritable>{
        Logger logger = LoggerFactory.getLogger(AnchorVideoMapper.class);
        Text k2 = new Text();
        VideoInfoWritable v2 = new VideoInfoWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            JSONObject jsonObj = JSON.parseObject(line);
            k2.set(jsonObj.getString("uid"));
            v2.set(jsonObj.getLong("gold"),jsonObj.getLong("watchnumpv"),
                    jsonObj.getLong("follower"),jsonObj.getLong("length"));
            context.write(k2,v2);
        }
    }

    public static class AnchorVideoReducer extends Reducer<Text,VideoInfoWritable,Text,VideoInfoWritable>{
        VideoInfoWritable v3 = new VideoInfoWritable();
        @Override
        protected void reduce(Text k2, Iterable<VideoInfoWritable> v2s, Context context)
                throws IOException, InterruptedException {
            long gold = 0L;
            long watchnumpv = 0L;
            long follower = 0L;
            long length = 0L;
            for (VideoInfoWritable v2 : v2s) {
                gold += v2.getGold();
                watchnumpv += v2.getWatchnumpv();
                follower += v2.getFollower();
                length += v2.getLength();
            }
            v3.set(gold,watchnumpv,follower,length);
            context.write(k2,v3);
        }
    }


    public static class VideoInfoWritable implements Writable{
        private Long gold;
        private Long watchnumpv;
        private Long follower;
        private Long length;

        public Long getGold() {
            return gold;
        }
        public Long getWatchnumpv() {
            return watchnumpv;
        }
        public Long getFollower() {
            return follower;
        }
        public Long getLength() {
            return length;
        }
        public void set(Long gold, Long watchnumpv, Long follower, Long length){
            this.gold = gold;
            this.watchnumpv = watchnumpv;
            this.follower = follower;
            this.length = length;
        }

        public void write(DataOutput out) throws IOException {
            out.writeLong(gold);
            out.writeLong(watchnumpv);
            out.writeLong(follower);
            out.writeLong(length);
        }

        public void readFields(DataInput in) throws IOException {
            this.gold = in.readLong();
            this.watchnumpv = in.readLong();
            this.follower = in.readLong();
            this.length = in.readLong();
        }

        @Override
        public String toString() {
            return gold +"\t" + watchnumpv +"\t" + follower +"\t" + length;
        }
    }

}
