package com.tfidf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

public class LastMapper extends Mapper<LongWritable, Text,Text,Text> {

    //文章总数
    public static Map<String,Integer> camp = null;
    //存放df
    public static Map<String,Integer> df = null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        if(camp == null || camp.size() == 0 || df == null || df.size() == 0){
            URI[] cacheFiles = context.getCacheFiles();
            if(cacheFiles != null){
                for (URI uri : cacheFiles) {
                    if(uri.getPath().endsWith("part-r-00003")){
                        //count 100
                        Path path = new Path(uri.getPath());
                        FileReader fileReader = new FileReader(path.getName());
                        BufferedReader bufferedReader = new BufferedReader(fileReader);
                        String line = bufferedReader.readLine();
                        if(line.contains("count")){
                            String[] split = line.split("\t");
                            camp = new HashMap<>();
                            camp.put(split[0],Integer.parseInt(split[1]));
                        }
                        bufferedReader.close();
                        fileReader.close();
                    }else {// 词条的DF
                        //豆浆  10
                        df = new HashMap<String, Integer>();
                        Path path = new Path(uri.getPath());
                        FileReader fileReader = new FileReader(path.getName());
                        BufferedReader bufferedReader = new BufferedReader(fileReader);
                        String line = null;
                        while ((line = bufferedReader.readLine()) != null){
                            String[] split = line.split("\t");
                            df.put(split[0],Integer.parseInt(split[1]));
                        }
                        bufferedReader.close();
                        fileReader.close();
                    }
                }
            }

        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //豆浆_3823930429533207	2
        FileSplit fs = (FileSplit) context.getInputSplit();
        if(!fs.getPath().getName().contains("part-r-00003")){
            String[] split = value.toString().split("\t");
            if(split.length >=2){
                int tf = Integer.parseInt(split[1]);
                String[] ss = split[0].split("_");
                String w = ss[0];
                String id = ss[1];
                double v = tf * (Math.log(camp.get("count") / df.get(w)));
                NumberFormat format = NumberFormat.getInstance();
                format.setMaximumFractionDigits(5);
                String tfIdf = format.format(v);
                context.write(new Text(id),new Text(w+":"+tfIdf));
            }
        }

    }
}
