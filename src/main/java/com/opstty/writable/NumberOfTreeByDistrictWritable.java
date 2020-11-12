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

    public NumberOfTreeByDistrictWritable(Text District, IntWritable Age) {
        district = District;
        number = Age;
    }
    public void write(DataOutput output) throws IOException {
        int D = new Integer(String.valueOf(district));
        int N = new Integer(String.valueOf(number));
        output.writeInt(D);
        output.writeInt(N);
    }

    public void read(DataInput input) throws IOException {
        district = new Text(input.readLine());
        number = new IntWritable(input.readInt());
    }

    public Text getDistrict() {
        return district;
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


}
