package com.opstty.job;

import com.opstty.mapper.DistrictContainingOldestTreeMapper;
import com.opstty.mapper.NumberOfTreesBySpeciesMapper;
import com.opstty.reducer.DistrictContainingOldestTreeReducer;
import com.opstty.reducer.NumberOfTreesBySpeciesReducer;
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

public class DistrictContainingOldestTree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: DistrictContainingOldestTree <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "DistrictContainingOldestTree");
        job.setJarByClass(DistrictContainingOldestTree.class);
        job.setMapperClass(DistrictContainingOldestTreeMapper.class);
        //job.setCombinerClass(DistrictContainingOldestTreeReducer.class);
        job.setReducerClass(DistrictContainingOldestTreeReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(OldestTreePerDistrictWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
