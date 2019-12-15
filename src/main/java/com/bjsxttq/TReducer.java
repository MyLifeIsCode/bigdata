package com.bjsxttq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//1990-10-01 34
//1990-10-02 33
//1991-10-04 32
public class TReducer extends Reducer<Tq, IntWritable, Text,IntWritable> {

    Text tkey = new Text();
    IntWritable tval = new IntWritable();
    @Override
    protected void reduce(Tq key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int flag = 0;
        int day = 0;
        for (IntWritable val : values){
            if(flag == 0){
                tkey.set(key.toString());
                tval.set(val.get());
                context.write(tkey,tval);
                flag ++;
                day = key.getDay();
            }
            if(flag != 0 && day != key.getDay()){
                tkey.set(key.toString());
                tval.set(val.get());
                context.write(tkey,tval);
                return;
            }
        }


    }
}
