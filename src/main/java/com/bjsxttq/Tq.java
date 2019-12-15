package com.bjsxttq;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Tq implements WritableComparable<Tq> {
    private int year;
    private int month;
    private int day;
    private int wd;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getWd() {
        return wd;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }

    @Override
    public int compareTo(Tq o) {
        int c1 = Integer.compare(this.getYear(),o.getYear());
        if(c1 == 0){
            int c2 = Integer.compare(this.getMonth(), o.getMonth());
            if(c2 == 0){
                return Integer.compare(this.getDay(),o.getDay());
            }
            return c2;
        }
        return c1;
    }

    //fix ***** 写的顺序和读的顺序必须相同
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.getYear());
        dataOutput.writeInt(this.getMonth());
        dataOutput.writeInt(this.getDay());
        dataOutput.writeInt(this.getWd());

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.setYear(dataInput.readInt());
        this.setMonth(dataInput.readInt());
        this.setDay(dataInput.readInt());
        this.setWd(dataInput.readInt());
    }

    @Override
    public String toString() {
        return  year + "-" + month + "-" +  day ;
    }
}
