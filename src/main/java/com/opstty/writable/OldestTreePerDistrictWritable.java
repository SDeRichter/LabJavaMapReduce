package com.opstty.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OldestTreePerDistrictWritable implements Writable {
    private IntWritable district;
    private IntWritable age;

    public OldestTreePerDistrictWritable() {
        district = new IntWritable(1);
        age = new IntWritable(0);
    }

    public OldestTreePerDistrictWritable(IntWritable District, IntWritable Age) {
        district = District;
        age = Age;
    }
    public void write(DataOutput output) throws IOException {
        int D = new Integer(String.valueOf(district));
        int A = new Integer(String.valueOf(age));
        output.writeInt(D);
        output.writeInt(A);
    }

    public void read(DataInput input) throws IOException {
        district = new IntWritable(input.readInt());
        age = new IntWritable(input.readInt());
    }

    public IntWritable getDistrict() {
        return district;
    }

    public IntWritable getAge() {
        return age;
    }

    public void setDistrict(IntWritable D) {
        district = D;
    }

    public void setAge(IntWritable A) {
        age = A;
    }

    public void readFields(DataInput dataInput) throws IOException {
        district.readFields(dataInput); //read district
        age.readFields(dataInput); //read age

    }


}
