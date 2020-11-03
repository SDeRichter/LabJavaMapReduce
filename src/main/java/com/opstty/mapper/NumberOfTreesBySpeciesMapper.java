package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NumberOfTreesBySpeciesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //only the first line of out csv contains GEOPOINT and it is useless so we remove values containing geopoint
        if(!value.toString().contains("GEOPOINT")){
            Text specie = new Text(value.toString().split(";")[3]);
            context.write(specie,one);
        }
    }
}
