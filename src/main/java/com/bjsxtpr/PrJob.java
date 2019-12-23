package com.bjsxtpr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//A	B	D
public class PrJob {

    public static void main(String[] args) {

        Configuration conf = new Configuration(true);
        conf.set("mapreduce.app-submission.corss-paltform", "true");
        //如果分布式运行,必须打jar包
        //且,client在集群外非hadoop jar 这种方式启动,client中必须配置jar的位置
        conf.set("mapreduce.framework.name", "local");
        //这个配置,只属于,切换分布式到本地单进程模拟运行的配置
        //这种方式不是分布式,所以不用打jar包


        double d = 0.0000001;
        int i = 0;
        while (true) {
            i++;
            try {
                conf.setInt("runCount", i);

                FileSystem fs = FileSystem.get(conf);
                Job job = Job.getInstance(conf);
                job.setJarByClass(PrJob.class);
                job.setJobName("pr" + i);
                job.setMapperClass(PrMapper.class);
                job.setReducerClass(PrReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                //使用了新的输入格式化类
                job.setInputFormatClass(KeyValueTextInputFormat.class);

                Path inputPath = new Path("/data/pagerank/input/");

                if (i > 1) {
                    inputPath = new Path("/data/pagerank/output/pr" + (i - 1));
                }
                FileInputFormat.addInputPath(job, inputPath);

                Path outpath = new Path("/data/pagerank/output/pr" + i);
                if (fs.exists(outpath)) {
                    fs.delete(outpath, true);
                }
                FileOutputFormat.setOutputPath(job, outpath);

                boolean f = job.waitForCompletion(true);
                if (f) {
                    System.out.println("success.");
                    long sum = job.getCounters().findCounter(Mycounter.my).getValue();

                    System.out.println(sum);
                    double avgd = sum / 4000.0;
                    if (avgd < d) {
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
