package com.tempus.graph;

import java.sql.*;
import java.util.UUID;


/**
 * Created by himanshu on 6/9/17.
 */
public class FacilityAssetInserter {
    public static void main(String[] args) {
        // Create variables
        Connection connection = null;
        Statement statement1 = null;
        Statement statement2 = null;
        Statement statement3 = null;
        Statement statement4 = null;
        Statement statement5 = null;
        ResultSet rs = null;
        PreparedStatement ps = null;

        try {
            // Connect to the database
            connection = DriverManager.getConnection("jdbc:phoenix:atlhashdn01.hashmap.net," +
                    "atlhashdn02.hashmap.net,atlhashdn03.hashmap.net:2181:/hbase-unsecure");
            connection.setAutoCommit(true);

            // Create a JDBC statement
            int itr = 0;
            String facilityID = "'2d45a1a8-5c2f-43a0-b1cc-0b9f3cd2ceab'";
            while (itr < 1000000) {

                UUID uuid2 = UUID.randomUUID();
                String assetId = uuid2.toString();
                statement1 = connection.createStatement();
                statement2 = connection.createStatement();

                // Execute our statements
                //statement.executeUpdate("create table javatest (mykey integer not null primary key, mycolumn varchar)");

                statement1.executeUpdate("upsert into  asset_test values ('A"+itr+"','" +
                        assetId+ "'," + facilityID + ",'NA','pump','Pune','NA')");
                itr++;
            }
            itr = 0;
            facilityID = "'cd8eb536-b4a5-4300-9628-f280daba9560'";
            while (itr < 10000000) {

                UUID uuid2 = UUID.randomUUID();
                String assetId = uuid2.toString();
                statement1 = connection.createStatement();
                statement2 = connection.createStatement();

                // Execute our statements
                //statement.executeUpdate("create table javatest (mykey integer not null primary key, mycolumn varchar)");

                statement1.executeUpdate("upsert into  asset_test values ('A"+itr+"','" +
                        assetId+ "'," + facilityID + ",'NA','pump','Pune','NA')");
                itr++;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (Exception e) {
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                }
            }
            if (statement1 != null) {
                try {
                    statement1.close();
                } catch (Exception e) {
                }
            }
            if (statement2 != null) {
                try {
                    statement2.close();
                } catch (Exception e) {
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                }
            }
        }
    }
}
