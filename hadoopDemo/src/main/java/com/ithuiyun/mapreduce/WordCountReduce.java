package com.ithuiyun.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ithuiyun.com on 2018/4/28.
 */
public class WordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
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
