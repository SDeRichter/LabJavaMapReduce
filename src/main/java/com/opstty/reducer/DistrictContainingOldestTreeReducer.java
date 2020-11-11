package com.opstty.reducer;

import com.opstty.writable.OldestTreePerDistrictWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DistrictContainingOldestTreeReducer extends Reducer<IntWritable, OldestTreePerDistrictWritable, Integer, Integer> {
    private OldestTreePerDistrictWritable result = new OldestTreePerDistrictWritable();

    public void reduce(IntWritable key,Iterable<OldestTreePerDistrictWritable> values,Context context)
            throws IOException,InterruptedException{
        for (OldestTreePerDistrictWritable current: values) {
            if (current.getAge() > result.getAge()) {
                result.setAge(new Integer(current.getAge()));
                result.setDistrict(new Integer(current.getDistrict()));
            }
        }
        context.write(result.getDistrict(),result.getAge());
    }
}
