package com.ithuiyun.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by ithuiyun.com on 2018/5/17.
 */
public class HiveJdbcDemo {
    public static void main(String[] args) throws Exception{
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String jdbcUrl = "jdbc:hive2://hadoop100:10000/default";
        Connection con = DriverManager.getConnection(jdbcUrl, "", "");
        Statement stmt = con.createStatement();
        String querySql = "select * from test";
        ResultSet res = stmt.executeQuery(querySql);
        while(res.next()){
            System.out.println(res.getInt("id")+"\t"+res.getString("name"));
        }
    }
}
