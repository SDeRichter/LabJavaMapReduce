package com.opstty.reducer;

import com.opstty.writable.NumberOfTreeByDistrictWritable;
import com.opstty.writable.OldestTreePerDistrictWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DistrictWithMostTreesReducer extends Reducer<IntWritable, NumberOfTreeByDistrictWritable, NullWritable, Text> {

    public void reduce(IntWritable key,Iterable<NumberOfTreeByDistrictWritable> values,Context context)
            throws IOException,InterruptedException{
        Text result=new Text("");
        int nombre= -1;

        for (NumberOfTreeByDistrictWritable current: values) {
            if (current.getNumber().get() > nombre) {
                nombre=current.getNumber().get();
                result=new Text(current.getDistrict().toString());
            }
        }
        context.write(NullWritable.get(),result);
    }
}
