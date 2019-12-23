package com.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

//输出单词数量  文章数量
public class FirstMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //3823890201582094	今天我约了豆浆
        String[] split = value.toString().split("\t");
        if(split.length > 1){
            String content = split[1].trim();
            StringReader sr = new StringReader(content);
            IKSegmenter ikSegmenter = new IKSegmenter(sr,true);
            Lexeme word = null;
            while ((word = ikSegmenter.next()) != null){
                String lexemeText = word.getLexemeText();
                context.write(new Text(lexemeText+"_" + split[0]),new IntWritable(1));
                //今天_3823890210294392	1
            }
        }
        context.write(new Text("count"),new IntWritable(1));

    }
}
