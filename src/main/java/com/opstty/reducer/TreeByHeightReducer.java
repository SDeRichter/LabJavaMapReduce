package com.opstty.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import sun.rmi.runtime.NewThreadAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class TreeByHeightReducer extends Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(NullWritable key,Iterable<DoubleWritable> values,Context context)
            throws IOException,InterruptedException{
        List<Double> l = new ArrayList();
        for(DoubleWritable val:values){
            l.add(val.get());
        }
        Collections.sort(l);
        for(Double val : l){
            result.set(val);
            context.write(NullWritable.get(), result);

        }
    }
}