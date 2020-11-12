package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

import static org.apache.commons.lang3.StringUtils.isNumeric;

public class TreeByHeightMapper extends Mapper<LongWritable, Text, NullWritable, DoubleWritable> {


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //only the first line of out csv contains GEOPOINT and it is useless so we remove values containing geopoint
        if(!value.toString().contains("GEOPOINT")){
            String[] array= value.toString().split(";");
            Text specie = new Text(array[3]);
            if (array[6]!=null && array[6].length()>0) {
                DoubleWritable result = new DoubleWritable(Double.parseDouble(array[6]));
                context.write(NullWritable.get(), result);
            }
            else{
                //nothing
            }
        }
    }
}
