package com.opstty.reducer;

import com.opstty.writable.NumberOfTreeByDistrictWritable;
import com.opstty.writable.OldestTreePerDistrictWritable;
import org.apache.hadoop.io.DoubleWritable;
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
public class DistrictWithMostTreesReducerTest {
    @Mock
    private Reducer.Context context;
    private DistrictWithMostTreesReducer districtWithMostTreesReducer;

    @Before
    public void setup() {
        this.districtWithMostTreesReducer = new DistrictWithMostTreesReducer();
    }

    @org.junit.Test
    public void testMap() throws IOException, InterruptedException {
        IntWritable zero = new IntWritable(0);
        NumberOfTreeByDistrictWritable v1 = new NumberOfTreeByDistrictWritable(new Text("7"),new IntWritable(5));
        NumberOfTreeByDistrictWritable v2 = new NumberOfTreeByDistrictWritable(new Text("5"),new IntWritable(7));
        NumberOfTreeByDistrictWritable v3 = new NumberOfTreeByDistrictWritable(new Text("3"),new IntWritable(3));

        Iterable<NumberOfTreeByDistrictWritable> values = Arrays.asList(v1,v2,v3);

        this.districtWithMostTreesReducer.reduce(zero, values, this.context);
        verify(this.context, times(1))
                .write(NullWritable.get(),new Text("5"));
    }
}