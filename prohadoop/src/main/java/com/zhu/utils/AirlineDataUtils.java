package com.zhu.utils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import com.zhu.c6.*;

public class AirlineDataUtils{
    public static String[] getSelectResultsPerRow(Text row){
        String[] contents = row.toString().split(",");
        String[] outputArray = new String[10];
        outputArray[0] = AirlineDataUtils.getDate(contents);
        outputArray[1] = AirlineDataUtils.getDepartureTime(contents);
        outputArray[2] = AirlineDataUtils.getArrivalTime(contents);
        outputArray[3] = AirlineDataUtils.getOrigin(contents);
        outputArray[4] = AirlineDataUtils.getDestination(contents);
        outputArray[5] = AirlineDataUtils.getDistance(contents);
        outputArray[6] = AirlineDataUtils.getElapsedTime(contents);
        outputArray[7] = AirlineDataUtils.getScheduledElapsedTime(contents);
        outputArray[8] = AirlineDataUtils.getDepartureDelay(contents);
        outputArray[9] = AirlineDataUtils.getArrivalDelay(contents);
        return outputArray;
    }

    public static DelaysWritable parseDelaysWritable(String line){
        String[] contents = line.split(",");
        DelaysWritable dw = new DelaysWritable();
        dw.year = new IntWritable(Integer.parseInt(AirlineDataUtils.getYear(contents)));
        dw.month = new IntWritable(Integer.parseInt(AirlineDataUtils.getMonth(contents)));
        dw.date = new IntWritable(Integer.parseInt(AirlineDataUtils.getDateOfMonth(contents)));
        dw.dayOfWeek = new IntWritable(Integer.parseInt(AirlineDataUtils.getDayOfTheWeek(contents)));
        dw.arrDelay = new IntWritable(AirlineDataUtils.parseMinutes(AirlineDataUtils.getArrivalDelay(contents), 0));
        dw.depDelay = new IntWritable(AirlineDataUtils.parseMinutes(AirlineDataUtils.getDepartureDelay(contents), 0));
        dw.destAirportCode = new Text(AirlineDataUtils.getDestination(contents));
        dw.carrierCode = new Text(AirlineDataUtils.getUniqueCarrier(contents));
        dw.originAirportCode = new Text(AirlineDataUtils.getOrigin(contents));
        return dw;
    }

    public static Text parseDelaysWritableToText(DelaysWritable val){
        StringBuilder tmp = new StringBuilder(50);
        tmp.append(val.year.get() + ",");
        tmp.append(val.month.get() + ",");
        tmp.append(val.date.get() + ",");
        tmp.append(val.dayOfWeek.get() + ",");
        tmp.append(val.carrierCode.toString());
        return new Text(tmp.toString());
    }
    public static Text parseDelaysWritableToText(DelaysWritable dw, String originAirportDesc, String destAirportDesc, String carrierDesc){
        return new Text(AirlineDataUtils.parseDelaysWritableToText(dw).toString() + ","
                + originAirportDesc + ","
                + destAirportDesc + ","
                + carrierDesc);
    }

    public static int getCustomPartition(MonthDoWWritable key, int indexRange, int noOfReducers){
        int indicesPerReducer = (int)Math.floor(indexRange / noOfReducers);
        int index = (key.month.get() - 1)*7 + (7-key.dayOfWeek.get());
        if(indexRange < noOfReducers){
            return index;
        }

        for(int i=0; i<noOfReducers; i++){
            int minValForPartitionInclusive = (i) * indicesPerReducer;
            int maxValForPartitionExclusive = (i+1) * indicesPerReducer;
            if(index >= minValForPartitionInclusive && index < maxValForPartitionExclusive){
                return i;
            }
        }
        return (noOfReducers - 1);
    }
    public static String getDateOfMonth(String[] contents){
        return contents[2];
    }

    public static String getDayOfTheWeek(String[] contents){
        return contents[3];
    }

    public static String getUniqueCarrier(String[] contents){
        return contents[8];
    }

    public static String getYear(String[] contents){
        return contents[0];
    }

    public static int parseMinutes(String minutes, int defaultValue){
        try{
            return Integer.parseInt(minutes);
        }catch(NumberFormatException e){
            return defaultValue;
        }
    }

    public static boolean isHeader(Text line){
        if(line.toString().contains("Year"))
            return true;
        else
            return false;
    }

    public static StringBuilder mergeStringArray(String[] fields, String sep){
        StringBuilder tmp = new StringBuilder(50);
        for(int i=0; i<fields.length; i++){
            tmp.append(fields[i]+sep);
        }
        return tmp.deleteCharAt(tmp.length()-1);
    }
    public static String getDate(String[] contents){
        return contents[1]+"/"+contents[2]+"/"+contents[0];
    }
    public static String getMonth(String[] contents){
        String month = "";
        if(Integer.parseInt(contents[1]) < 10)
            month = "0"+contents[1];
        else
            month = contents[1];
        return month;
    }
    public static String getDepartureTime(String[] contents){
        return contents[4];
    }
    public static String getArrivalTime(String[] contents){
        return contents[6];
    }
    public static String getOrigin(String[] contents){
        return contents[16];
    }
    public static String getDestination(String[] contents){
        return contents[17];
    }
    public static String getDistance(String[] contents){
        return contents[18];
    }
    public static String getElapsedTime(String[] contents){
        return contents[11];
    }
    public static String getScheduledElapsedTime(String[] contents){
        return contents[12];
    }
    public static String getDepartureDelay(String[] contents){
        return contents[15];
    }
    public static String getArrivalDelay(String[] contents){
        return contents[14];
    }

    public static String getCancelled(String[] contents){
        return contents[21];
    }

    public static String getDiverted(String[] contents){
        return contents[23];
    }

    public static boolean parseBoolean(String buer, boolean defaultValue){
        if(buer.contains("NA")){
            return defaultValue;
        }

        boolean flage = defaultValue;
        try{
            int tmp = Integer.parseInt(buer);
            if(tmp == 1)
                flage = true;
        }catch(NumberFormatException e){
            return defaultValue;
        }
        return flage;
    }

    public static boolean isCarrierFileHeader(String line){
        if(line.contains("Code"))
            return true;
        else
            return false;
    }

    public static String[] parseCarrierLine(String line){
        return line.split(",");
    }
}
