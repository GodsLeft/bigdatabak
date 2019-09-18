package com.zhu.utils;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.apache.hadoop.io.Text;

public class AirlineDataUtilsTest{
    public static String line = "1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA";

    @Test
    public void mergeStringArrayTest(){
        String[] fields = line.split(",");
        StringBuilder tmp = AirlineDataUtils.mergeStringArray(AirlineDataUtils.getSelectResultsPerRow(new Text(line)), ",");
        System.out.println(tmp.toString());
    }

    @Test
    public void parseMinutesTest(){
        String minutes = "77aa";
        System.out.println(AirlineDataUtils.parseMinutes(minutes, 5));
    }

}
