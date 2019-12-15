package com.bjsxtfd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
//tom hadoop hdfs
public class FdMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text tkey = new Text();
    IntWritable tval = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = StringUtils.split(value.toString(), ' ');
        for (int i = 1; i < words.length; i++) {
            tkey.set(getKey(words[0],words[1]));
            tval.set(0);
            context.write(tkey,tval);
            for (int j  = i+1; j < words.length; j++) {
                tkey.set(getKey(words[i],words[j]));
                tval.set(1);
                context.write(tkey,tval);
            }
        }
    }

    private Text getKey(String a, String b) {
        return new Text(a.compareTo(b) < 0 ? a + ":" + b : b + ":" + a);
    }
}
