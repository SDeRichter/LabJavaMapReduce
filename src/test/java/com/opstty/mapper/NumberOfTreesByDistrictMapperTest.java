package com.opstty.mapper;

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
public class NumberOfTreesByDistrictMapperTest {
    @Mock
    private Mapper.Context context;
    private NumberOfTreesByDistrictMapper numberOfTreesByDistrictMapper;

    @Before
    public void setup() {
        this.numberOfTreesByDistrictMapper = new NumberOfTreesByDistrictMapper();
    }

    @org.junit.Test
    public void testMap() throws IOException, InterruptedException {
        String value = "(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars";
        this.numberOfTreesByDistrictMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("7"), new IntWritable(1));
    }
}