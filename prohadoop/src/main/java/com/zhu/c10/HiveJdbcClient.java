package com.zhu.c10;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class HiveJdbcClient{
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception{
        Class.forName(driverName);
        Connection con = DriverManager.getConnection("jdbc:hive://master:3306", "hive", "hive");
        Statement stmt = con.createStatement();

        String sql = "show tables";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if(res.next()){
            System.out.println(res.getString(1));
        }
    }
}
