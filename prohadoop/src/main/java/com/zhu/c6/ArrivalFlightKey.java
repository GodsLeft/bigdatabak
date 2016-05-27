package com.zhu.c6;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
public class ArrivalFlightKey implements WritableComparable<ArrivalFlightKey> {
    public Text destinationAirport = new Text("");
    public Text arrivalDtTime = new Text("");

    public ArrivalFlightKey(){}

    public ArrivalFlightKey(Text destinationAirport, Text arrivalDtTime){
        this.destinationAirport = destinationAirport;
        this.arrivalDtTime = arrivalDtTime;
    }

    @Override
    public int hashCode(){
        return (this.destinationAirport).hashCode();
    }

    @Override
    public boolean equals(Object o){
        if(!(o instanceof ArrivalFlightKey))
            return false;
        ArrivalFlightKey other = (ArrivalFlightKey)o;
        return this.destinationAirport.equals(other.destinationAirport);
    }

    @Override
    public int compareTo(ArrivalFlightKey second){
        return this.destinationAirport.compareTo(second.destinationAirport);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        destinationAirport.write(out);
        arrivalDtTime.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        destinationAirport.readFields(in);
        arrivalDtTime.readFields(in);
    }
}
