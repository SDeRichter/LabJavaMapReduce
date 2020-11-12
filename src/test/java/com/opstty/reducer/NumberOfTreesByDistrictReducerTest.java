package com.opstty.reducer;

import com.opstty.writable.NumberOfTreeByDistrictWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class NumberOfTreesByDistrictReducerTest {
    @Mock
    private Reducer.Context context;
    private NumberOfTreesByDistrictReducer numberOfTreesByDistrictReducer;

    @Before
    public void setup() {
        this.numberOfTreesByDistrictReducer = new NumberOfTreesByDistrictReducer();
    }

    @org.junit.Test
    public void testMap() throws IOException, InterruptedException {
        Text key = new Text("7");

        IntWritable v1 = new IntWritable(1);
        Iterable<IntWritable> values = Arrays.asList(v1,v1,v1);

        this.numberOfTreesByDistrictReducer.reduce(key, values, this.context);
        verify(this.context, times(1))
                .write(key, new IntWritable(3));
    }
}