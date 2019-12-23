package com.bjsxtpr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PrReducer extends Reducer<Text, Text,Text,Text> {

    //A 1.0 B   D
    //A 0.1
    //A 0.2
    //相同的key会进入同一个reducer,停止也只是停止一个reducer
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Node sourceNode = null;
        double sum = 0.0;
//        System.out.println("=================="+key.toString());

        for (Text value : values) {
//            System.out.println("=================="+value.toString());
            Node node = Node.formatNode(value.toString());
            if (node.constainsAdjances()) {
                sourceNode = node;
            }else {
                sum = sum + node.getPageRank();
            }
        }
        double newPR = (0.15/4) + (0.85 * sum);
        double d = Math.abs(sourceNode.getPageRank() - newPR);
        int j = (int)(d * 1000.0);
        context.getCounter(Mycounter.my).increment(j);
        sourceNode.setPageRank(newPR);
        context.write(key,new Text(sourceNode.toString()));
    }
}
