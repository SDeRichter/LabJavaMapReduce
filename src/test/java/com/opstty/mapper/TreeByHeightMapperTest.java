package com.opstty.mapper;

import com.opstty.writable.NumberOfTreeByDistrictWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
public class TreeByHeightMapperTest {
    @Mock
    private Mapper.Context context;
    private TreeByHeightMapper treeByHeightMapper;

    @Before
    public void setup() {
        this.treeByHeightMapper = new TreeByHeightMapper();
    }

    @org.junit.Test
    public void testMap() throws IOException, InterruptedException {
        String value = "(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars";
        this.treeByHeightMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(NullWritable.get(), new DoubleWritable(13.0));
    }
}