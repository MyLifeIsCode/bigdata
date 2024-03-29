package com.bjsxttq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyTQ {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 、 配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyTQ.class);
        job.setJobName("tq");

        //2、设置 输入输出路径
        Path inPath = new Path("/tq/input");
        FileInputFormat.addInputPath(job,inPath);
        Path outPath = new Path("/tq/output");
        if(outPath.getFileSystem(conf).exists(outPath))
            outPath.getFileSystem(conf).delete(outPath,true);
        FileOutputFormat.setOutputPath(job,outPath);

        //3、设置mapper
        job.setMapperClass(Tmapper.class);
        job.setMapOutputKeyClass(Tq.class);
        job.setMapOutputValueClass(IntWritable.class);

        //4、自定义排序比较器
        job.setSortComparatorClass(TSortComparator.class);

        //5、自定义分区器
        job.setPartitionerClass(TPartioner.class);

        //6、（由于TSortComparator，reduce端会把相同年月温度作为一组处理）需要自定义组排序器
        job.setGroupingComparatorClass(TGroupComparator.class);
        //7、设置reducetask数量
        job.setNumReduceTasks(2);
        //8、设置reduce
        job.setReducerClass(TReducer.class);

        //9、提交
        job.waitForCompletion(true);
    }
}
