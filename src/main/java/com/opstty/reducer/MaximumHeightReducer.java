package com.opstty.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class MaximumHeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
private DoubleWritable result = new DoubleWritable();

public void reduce(Text key,Iterable<DoubleWritable> values,Context context)
        throws IOException,InterruptedException{
        double max=0;
        for(DoubleWritable val:values){
            if(val.get()>max) {
                max=val.get();
            }
        }
        result.set(max);
        context.write(key,result);
        }
}