package com.ithuiyun.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 针对某一些key是已经提供好的，
 * 可以通过自定义排序类实现排序
 *
 * Created by ithuiyun.com on 2018/4/28.
 */
public class SortWordCountApp2 {
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
        String jobName = SortWordCountApp2.class.getSimpleName();
        //把所有的内容都封装到job中
        Job job = Job.getInstance(conf, jobName);
        //程序打jar包必备代码
        job.setJarByClass(SortWordCountApp2.class);


        //设置输入路径
        FileInputFormat.setInputPaths(job,inputPath);
        //设置输出路径
        FileOutputFormat.setOutputPath(job,outputDir);

        //设置自定义mapper类
        job.setMapperClass(SortMapper.class);
        //指定k2,v2类型
        job.setMapOutputKeyClass(TwoInt.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setSortComparatorClass(MySortCompatator.class);

        //设置自定义reduce类型
        job.setReducerClass(SortReduce.class);
        //指定k3,v3类型
        job.setOutputKeyClass(TwoInt.class);
        job.setOutputValueClass(NullWritable.class);

        //提交任务，等待任务执行结束
        job.waitForCompletion(true);
    }

    public static class SortMapper extends Mapper<LongWritable,Text,TwoInt,NullWritable>{
        TwoInt twoInt = new TwoInt();
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            twoInt.set(Integer.parseInt(split[0]),Integer.parseInt(split[1]));
            context.write(twoInt,NullWritable.get());
        }
    }

    public static class SortReduce extends Reducer<TwoInt,NullWritable,TwoInt,NullWritable>{
        @Override
        protected void reduce(TwoInt key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static class MySortCompatator extends WritableComparator{
        public MySortCompatator(){
            super(TwoInt.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TwoInt aa = (TwoInt) a;
            TwoInt bb = (TwoInt) b;
            int res = aa.getFirst().compareTo(bb.getFirst());
            if(res == 0){
                return aa.getSecond().compareTo(bb.getSecond());
            }else{
                return res;
            }
        }
    }


    /**
     * key必须是支持排序的，所以必须要实现WritableComparable
     */
    public static class TwoInt implements WritableComparable<TwoInt>{

        private Integer first;
        private Integer second;

        public Integer getFirst() {
            return first;
        }

        public Integer getSecond() {
            return second;
        }

        public void set(Integer first, Integer second) {
            this.first = first;
            this.second = second;
        }

        /**
         * 覆盖compareTo方法
         * @param o
         * @return
         */
        @Override
        public int compareTo(TwoInt o) {
            return 0;
        }

        /**
         * 一般覆盖compatrTo的时候也会把equals和hashcode实现了
         * @param o
         * @return
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TwoInt twoInt = (TwoInt) o;

            if (first != null ? !first.equals(twoInt.first) : twoInt.first != null) return false;
            return second != null ? second.equals(twoInt.second) : twoInt.second == null;
        }

        @Override
        public int hashCode() {
            int result = first != null ? first.hashCode() : 0;
            result = 31 * result + (second != null ? second.hashCode() : 0);
            return result;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(first);
            out.writeInt(second);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.first = in.readInt();
            this.second = in.readInt();
        }

        @Override
        public String toString() {
            return  first +"\t" + second;
        }
    }


}



