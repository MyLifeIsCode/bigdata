package com.bjsxtfd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyFd {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1、配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyFd.class);
        job.setJobName("MyFd");

        //2、输入路径
        Path inPath = new Path("/fd/input");
        FileInputFormat.addInputPath(job,inPath);

        //3、设置输出路径
        Path outPath = new Path("/fd/output");
        if(outPath.getFileSystem(conf).exists(outPath)){
            outPath.getFileSystem(conf).delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

        //4、设置mapper
        job.setMapperClass(FdMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //5、 设置reduce
        job.setReducerClass(FdReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6、提交
        job.waitForCompletion(true);
    }
}
