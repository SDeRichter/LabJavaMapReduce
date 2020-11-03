package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DisWTreesReducerTest {
    @Mock
    private Reducer.Context context;
    private DisWTreesReducer DisWTreesReducer;

    @Before
    public void setup() {
        this.DisWTreesReducer = new DisWTreesReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "7";
        this.DisWTreesReducer.reduce(new Text(key), null, this.context);
        verify(this.context).write(new Text("7"), NullWritable.get());
    }
}
