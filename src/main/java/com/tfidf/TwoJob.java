package com.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TwoJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set("mapreduce.app-submission.corss-paltform", "true");
        //如果分布式运行,必须打jar包
        //且,client在集群外非hadoop jar 这种方式启动,client中必须配置jar的位置
        conf.set("mapreduce.framework.name", "local");
        //这个配置,只属于,切换分布式到本地单进程模拟运行的配置
        //这种方式不是分布式,所以不用打jar包

        Job job = Job.getInstance(conf);

        job.setJobName("twoJob");
        job.setJarByClass(TwoJob.class);

        job.setMapperClass(TwoMapper.class);
        job.setReducerClass(TwoReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path inputPath = new Path("/data/tfidf/output/weibo1");
        FileInputFormat.addInputPath(job,inputPath);
        Path outPath = new Path("/data/tfidf/output/weibo2");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

        boolean b = job.waitForCompletion(true);
        if(b){
            System.out.println("执行成功");
        }
    }
}
