package com.bjsxttq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartioner extends Partitioner<Tq, IntWritable> {

    /**
     *
     * @param tq
     * @param intWritable
     * @param numPartitions 和reducetask数量一致
     * @return
     */
    @Override
    public int getPartition(Tq tq, IntWritable intWritable, int numPartitions) {

        return tq.getYear() % numPartitions;
    }
}
