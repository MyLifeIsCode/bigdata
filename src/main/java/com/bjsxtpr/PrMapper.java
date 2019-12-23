package com.bjsxtpr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//key A  value	B	D  分隔符\t
public class PrMapper extends Mapper<Text, Text,Text,Text> {

    //A B   D
    //1.0
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        int runCount = context.getConfiguration().getInt("runCount", 1);

        String page = key.toString();

        Node node = null;
        if (runCount == 1) {
            node = Node.formatNode("1.0" , value.toString());
        } else {
            node = Node.formatNode(value.toString());
        }
        context.write(new Text(page), new Text(node.toString()));
        if(node.constainsAdjances()){
            double outValue = node.getPageRank() / node.getAdjacentNodeNames().length;

            for (String adjacentNodeName : node.getAdjacentNodeNames()) {
//                System.out.println("--------------------"+ adjacentNodeName +"------" +outValue);

                context.write(new Text(adjacentNodeName),new Text(outValue + ""));
            }
        }
    }
}






























