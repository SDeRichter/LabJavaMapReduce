package com.opstty.mapper;

import com.opstty.writable.OldestTreePerDistrictWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.lang.Integer.parseInt;

public class DistrictContainingOldestTreeMapper extends Mapper<LongWritable, Text, IntWritable, OldestTreePerDistrictWritable> {
    private final static IntWritable one = new IntWritable(1);
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //only the first line of out csv contains GEOPOINT and it is useless so we remove values containing geopoint
        if(!value.toString().contains("GEOPOINT")){
            String District = new String(value.toString().split(";")[1]);
            int district = parseInt(District);
            int age = new Integer(value.toString().split(";")[5]);
            age = 2020-age;
            OldestTreePerDistrictWritable values = new OldestTreePerDistrictWritable(district, age);
            context.write(one, values);
        }
    }

}
