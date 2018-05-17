package com.ithuiyun.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
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
