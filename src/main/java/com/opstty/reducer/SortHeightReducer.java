package com.opstty.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class SortHeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key,Iterable<DoubleWritable> values,Context context)
            throws IOException,InterruptedException{
        List<Double> l = new ArrayList();
        for(DoubleWritable val:values){
            l.add(val.get());
        }
        Collections.sort(l);
        for(Double val : l){
            result.set(val);
            if(result.get()!=-1) {
                context.write(key, result);
            }
        }
    }
}