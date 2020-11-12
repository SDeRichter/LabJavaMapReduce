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

public class DistrictWithMostTreesReducer extends Reducer<IntWritable, NumberOfTreeByDistrictWritable, Text, NullWritable> {
    private NumberOfTreeByDistrictWritable result = new NumberOfTreeByDistrictWritable();

    public void reduce(IntWritable key,Iterable<NumberOfTreeByDistrictWritable> values,Context context)
            throws IOException,InterruptedException{
        for (NumberOfTreeByDistrictWritable current: values) {
            if (current.getNumber().get() > result.getNumber().get()) {
                result.setNumber(new IntWritable(current.getNumber().get()));
                result.setDistrict(new Text(current.getDistrict()));
            }
        }
        context.write(result.getDistrict(),NullWritable.get());
    }
}
