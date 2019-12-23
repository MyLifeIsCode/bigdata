package com.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LastJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.corss-paltform", "true");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);

        //2.5
        //把微博总数加载到
        job.addCacheFile(new Path("/data/tfidf/output/weibo1/part-r-00003").toUri());
        //把df加载到
        job.addCacheFile(new Path("/data/tfidf/output/weibo2/part-r-00000").toUri());

        job.setMapperClass(LastMapper.class);
        job.setReducerClass(LastReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        job.setSortComparatorClass(LastComparator.class);
        Path inputPath = new Path("/data/tfidf/output/weibo1");
        FileInputFormat.addInputPath(job,inputPath);

        Path outPath = new Path("/data/tfidf/output/weibo3");
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
