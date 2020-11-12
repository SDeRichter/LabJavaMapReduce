package com.opstty.mapper;

import com.opstty.writable.OldestTreePerDistrictWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.lang.Integer.parseInt;
import static org.apache.commons.lang3.StringUtils.isNumeric;

public class DistrictContainingOldestTreeMapper extends Mapper<LongWritable, Text, IntWritable, OldestTreePerDistrictWritable> {
    private final static IntWritable zero = new IntWritable(1);
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //only the first line of out csv contains GEOPOINT and it is useless so we remove values containing geopoint
        if(!value.toString().contains("GEOPOINT")){
            String[] array= value.toString().split(";");
            Text District = new Text("-1");
            Integer age = new Integer(-1);
            if (array[1] != null && array[1].length()>0 && isNumeric(array[1]) && array[5]!=null && array[5].length()>0 && isNumeric(array[5]) ) {
                District = new Text(array[1]);
                IntWritable district = new IntWritable(parseInt(District.toString()));
                age = new Integer(array[5] );
                age = 2020-age;
                IntWritable Age = new IntWritable(age);
                OldestTreePerDistrictWritable values = new OldestTreePerDistrictWritable(district, Age);
                context.write(zero, values);
            }
            else
            {
                context.write(zero, new OldestTreePerDistrictWritable(zero, zero));
            }

        }
    }

}