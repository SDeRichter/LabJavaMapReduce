package com.opstty.mapper;

import com.opstty.writable.NumberOfTreeByDistrictWritable;
import com.opstty.writable.OldestTreePerDistrictWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DistrictWithMostTreesMapperTest {
    @Mock
    private Mapper.Context context;
    private DistrictWithMostTreesMapper districtWithMostTreesMapper;

    @Before
    public void setup() {
        this.districtWithMostTreesMapper = new DistrictWithMostTreesMapper();
    }

    @org.junit.Test
    public void testMap() throws IOException, InterruptedException {
        Text key = new Text("7");
        Text value = new Text("7\t5");
        IntWritable zero = new IntWritable(0);
        this.districtWithMostTreesMapper.map(key, value, this.context);
        ArgumentCaptor<NumberOfTreeByDistrictWritable> parameterCaptor = ArgumentCaptor.forClass(NumberOfTreeByDistrictWritable.class);
        verify(this.context, times(1))
                .write(eq(zero), parameterCaptor.capture()); //any(OldestTreePerDistrictWritable.class)
        assertEquals(key,parameterCaptor.getValue().getDistrict());
        assertEquals(new IntWritable(5),parameterCaptor.getValue().getNumber());
    }
}