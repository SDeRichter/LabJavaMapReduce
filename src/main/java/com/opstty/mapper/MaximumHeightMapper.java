package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

import static org.apache.commons.lang3.StringUtils.isNumeric;

public class MaximumHeightMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //only the first line of out csv contains GEOPOINT and it is useless so we remove values containing geopoint
        if(!value.toString().contains("GEOPOINT")){
            Text specie = new Text(value.toString().split(";")[3]);
            String height = value.toString().split(";")[7];
            if (isNumeric(height)){
                IntWritable result = new IntWritable(Integer.parseInt(height));
                context.write(specie,result);
            }


        }
    }
}
