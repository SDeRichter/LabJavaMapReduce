package com.opstty.writable;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OldestTreePerDistrictWritable {
    private int district;
    private int age;

    public OldestTreePerDistrictWritable() {
        district = 1;
        age = 0;
    }

    public OldestTreePerDistrictWritable(int District, int Age) {
        district = District;
        age = Age;
    }
    public void write(DataOutput output) throws IOException {
        output.writeInt(district);
        output.writeDouble(age);
    }

    public void read(DataInput input) throws IOException {
        district = input.readInt();
        age = input.readInt();
    }

    public int getDistrict() {
        return district;
    }

    public int getAge() {
        return age;
    }

    public void setDistrict(int D) {
        district = D;
    }

    public void setAge(int A) {
        age = A;
    }


}
