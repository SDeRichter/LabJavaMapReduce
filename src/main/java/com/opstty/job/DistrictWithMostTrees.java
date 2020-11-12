package com.opstty.job;

import com.opstty.mapper.DistrictContainingOldestTreeMapper;
import com.opstty.mapper.DistrictWithMostTreesMapper;
import com.opstty.mapper.NumberOfTreesByDistrictMapper;
import com.opstty.mapper.NumberOfTreesBySpeciesMapper;
import com.opstty.reducer.DistrictContainingOldestTreeReducer;
import com.opstty.reducer.DistrictWithMostTreesReducer;
import com.opstty.reducer.NumberOfTreesByDistrictReducer;
import com.opstty.reducer.NumberOfTreesBySpeciesReducer;
import com.opstty.writable.NumberOfTreeByDistrictWritable;
import com.opstty.writable.OldestTreePerDistrictWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictWithMostTrees {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Configuration conf2 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: DistrictWithMostTrees <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "numberoftreesbydistrict");
        Job job2 = Job.getInstance(conf2, "districtwithmosttrees");


        job.setJarByClass(DistrictWithMostTrees.class);
        job.setMapperClass(NumberOfTreesByDistrictMapper.class);
        //job.setCombinerClass(DistrictContainingOldestTreeReducer.class);
        job.setReducerClass(NumberOfTreesByDistrictReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        job.waitForCompletion(true);


        job2.setJarByClass(DistrictWithMostTrees.class);
        job2.setMapperClass(DistrictWithMostTreesMapper.class);
        //job.setCombinerClass(DistrictContainingOldestTreeReducer.class);
        job2.setReducerClass(DistrictWithMostTreesReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(NumberOfTreeByDistrictWritable.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2,new Path(otherArgs[otherArgs.length -1]));
        FileOutputFormat.setOutputPath(job2,
                new Path(otherArgs[otherArgs.length - 1]));





        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
