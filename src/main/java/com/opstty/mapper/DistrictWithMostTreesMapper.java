package com.opstty.mapper;

import com.opstty.writable.NumberOfTreeByDistrictWritable;
import com.opstty.writable.OldestTreePerDistrictWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.lang.Integer.parseInt;
import static org.apache.commons.lang3.StringUtils.isNumeric;

public class DistrictWithMostTreesMapper extends Mapper<Text, IntWritable, IntWritable, NumberOfTreeByDistrictWritable> {
    private final static IntWritable zero = new IntWritable(0);
    public void map(Text key, IntWritable value, Mapper.Context context) throws IOException, InterruptedException {
        //only the first line of out csv contains GEOPOINT and it is useless so we remove values containing geopoint

        NumberOfTreeByDistrictWritable pair = new NumberOfTreeByDistrictWritable(key,value);

        context.write(zero,pair);

    }

}