package com.bjsxtfd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
//hadoop:tom 0
//hadoop:tom 1
//hadoop:tom 1
//hadoop:tom 1

public class FdReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    IntWritable tval = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            if(value.get() == 0){
                return;
            }
            sum ++;
        }
        tval.set(sum);
        context.write(key,tval);
    }
}
