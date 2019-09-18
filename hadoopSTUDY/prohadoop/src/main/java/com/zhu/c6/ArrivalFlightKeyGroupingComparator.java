package com.zhu.c6;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
public class ArrivalFlightKeyGroupingComparator extends WritableComparator{
    public ArrivalFlightKeyGroupingComparator(){
        super(ArrivalFlightKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b){
        ArrivalFlightKey first = (ArrivalFlightKey) a;
        ArrivalFlightKey second = (ArrivalFlightKey) b;

        return first.destinationAirport.compareTo(second.destinationAirport);
    }
}
