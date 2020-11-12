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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@RunWith(MockitoJUnitRunner.class)
public class DistrictContainingOldestTreeMapperTest {
    @Mock
    private Mapper.Context context;
    private DistrictContainingOldestTreeMapper districtContainingOldestTreeMapper;

    @Before
    public void setup() {
        this.districtContainingOldestTreeMapper = new DistrictContainingOldestTreeMapper();
    }

    @org.junit.Test
    public void testMap() throws IOException, InterruptedException {
        ArgumentCaptor<OldestTreePerDistrictWritable> parameterCaptor = ArgumentCaptor.forClass(OldestTreePerDistrictWritable.class);
        String value = "(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars";
        int Age = 2020-1935;
        OldestTreePerDistrictWritable values = new OldestTreePerDistrictWritable(new IntWritable(7), new IntWritable(Age));
        this.districtContainingOldestTreeMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(eq(new IntWritable(0)), parameterCaptor.capture()); //any(OldestTreePerDistrictWritable.class)
        assertEquals(new IntWritable(7),parameterCaptor.getValue().getDistrict());
        assertEquals(new IntWritable(85),parameterCaptor.getValue().getAge());
    }
}