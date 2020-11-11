package com.opstty.reducer;

import com.opstty.mapper.NumberOfTreesBySpeciesMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class NumberOfTreesBySpeciesReducerTest {
    @Mock
    private Reducer.Context context;
    private NumberOfTreesBySpeciesReducer numberOfTreesBySpeciesReducer;

    @Before
    public void setup() {
        this.numberOfTreesBySpeciesReducer = new NumberOfTreesBySpeciesReducer();
    }

    @org.junit.Test
    public void testMap() throws IOException, InterruptedException {
        ArrayList<IntWritable> value = new ArrayList<IntWritable>();
        value.add(new IntWritable(1));
        value.add(new IntWritable(1));

        this.numberOfTreesBySpeciesReducer.reduce(new Text ("pomifera"), value, this.context);
        verify(this.context, times(1))
                .write(new Text("pomifera"), new IntWritable(2));
    }
}