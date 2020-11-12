package com.opstty.mapper;

import com.opstty.writable.NumberOfTreeByDistrictWritable;
import com.opstty.writable.OldestTreePerDistrictWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.lang.Integer.parseInt;
import static org.apache.commons.lang3.StringUtils.isNumeric;
import static org.apache.commons.lang3.StringUtils.split;

public class DistrictWithMostTreesMapper extends Mapper<Object, Text, IntWritable, NumberOfTreeByDistrictWritable> {
    private final static IntWritable zero = new IntWritable(0);
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        //only the first line of out csv contains GEOPOINT and it is useless so we remove values containing geopoint
        String[] array= value.toString().split("\t");
        NumberOfTreeByDistrictWritable pair = new NumberOfTreeByDistrictWritable(new Text(array[0]) ,new IntWritable(Integer.parseInt(array[1])));
        context.write(zero,pair);
    }
}