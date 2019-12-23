package com.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FirstReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //今天_3823890210294392	1
        //count 1
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
