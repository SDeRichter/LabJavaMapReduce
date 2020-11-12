package com.opstty.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumberOfTreeByDistrictWritable implements Writable {
    private Text district;
    private IntWritable number;

    public NumberOfTreeByDistrictWritable() {
        district = new Text("-1");
        number = new IntWritable(0);
    }

    public NumberOfTreeByDistrictWritable(Text District, IntWritable Number) {
        district = District;
        number = Number;
    }



    public Text getDistrict() {
        return district;
    }

    public void write(DataOutput dataOutput) throws IOException {
        district.write(dataOutput); //read district
        number.write(dataOutput); //read age
    }

    public IntWritable getNumber() {
        return number;
    }

    public void setDistrict(Text D) {
        district = D;
    }

    public void setNumber(IntWritable N) {
        number = N;
    }

    public void readFields(DataInput dataInput) throws IOException {
        district.readFields(dataInput); //read district
        number.readFields(dataInput); //read age
    }

    @Override
    public String toString(){
        return (getDistrict()+"   "+getNumber());
    }

}
