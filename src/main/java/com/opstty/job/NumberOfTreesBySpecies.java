package com.opstty.job;

import com.opstty.mapper.NumberOfTreesBySpeciesMapper;
import com.opstty.reducer.NumberOfTreesBySpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NumberOfTreesBySpecies {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: NumberOftreeBySpecies <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "numbertreebyspecies");
        job.setJarByClass(NumberOfTreesBySpecies.class);
        job.setMapperClass(NumberOfTreesBySpeciesMapper.class);
        job.setCombinerClass(NumberOfTreesBySpeciesReducer.class);
        job.setReducerClass(NumberOfTreesBySpeciesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
