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
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TreeSpeciesReducerTest {
    @Mock
    private Reducer.Context context;
    private TreeSpeciesReducer treeSpeciesReducer;

    @Before
    public void setup() {
        this.treeSpeciesReducer = new TreeSpeciesReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "pomifera";
        this.treeSpeciesReducer.reduce(new Text(key), null, this.context);
        verify(this.context).write(new Text("pomifera"), NullWritable.get());
    }
}
