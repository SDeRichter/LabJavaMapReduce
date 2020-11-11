package com.opstty.reducer;

import com.opstty.writable.OldestTreePerDistrictWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DistrictContainingOldestTreeReducer extends Reducer<IntWritable, OldestTreePerDistrictWritable, IntWritable, IntWritable> {
    private OldestTreePerDistrictWritable result = new OldestTreePerDistrictWritable();

    public void reduce(IntWritable key,Iterable<OldestTreePerDistrictWritable> values,Context context)
            throws IOException,InterruptedException{
        for (OldestTreePerDistrictWritable current: values) {
            if (current.getAge().get() > result.getAge().get()) {
                result.setAge(new IntWritable(current.getAge().get()));
                result.setDistrict(new IntWritable(current.getDistrict().get()));
            }
        }
        context.write(result.getDistrict(),result.getAge());
    }
}
