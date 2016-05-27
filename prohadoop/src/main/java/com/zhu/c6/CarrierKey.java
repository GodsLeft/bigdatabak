package com.zhu.c6;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class CarrierKey implements WritableComparable<CarrierKey>{
    public static final IntWritable TYPE_CARRIER = new IntWritable(0);
    public static final IntWritable TYPE_DATA = new IntWritable(1);
    public IntWritable type = new IntWritable(3);
    public Text code = new Text("");
    public Text desc = new Text("");

    public CarrierKey(){}
    public CarrierKey(IntWritable type, Text code, Text desc){
        this.type = type;
        this.code = code;
        this.desc = desc;
    }

    public CarrierKey(IntWritable type, Text code){
        this.type = type;
        this.code = code;
    }

    @Override
    public void write(DataOutput out) throws IOException{
        type.write(out);
        code.write(out);
        desc.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        type.readFields(in);
        code.readFields(in);
        desc.readFields(in);
    }

    @Override
    public int hashCode(){
        return (this.code.toString() + Integer.toString(this.type.get())).hashCode();
    }

    @Override
    public int compareTo(CarrierKey second){
        CarrierKey first = this;
        if(first.code.equals(second.code)){
            return first.type.compareTo(second.type);
        }else{
            return first.code.compareTo(second.code);
        }
    }
}
