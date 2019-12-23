package com.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

//词在多少文章中出现
public class TwoMapper extends Mapper<LongWritable,Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //豆浆_3823890201582094	3
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        if(!fileSplit.getPath().getName().contains("part-r-00003")){
            String[] split = value.toString().split("\t");

            String s = split[0];
            String[] ss = s.split("_");
            if(ss.length >= 2){
                String w = ss[0];
                String s1 = ss[1];
                context.write(new Text(w),new IntWritable(1));
            }else {
                System.out.println(value.toString() + "-------------");
            }
        }
    }
}
