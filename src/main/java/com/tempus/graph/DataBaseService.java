package com.tempus.graph;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by himanshu on 29/8/17.
 */
public class DataBaseService {
    Connection connection = null;
    PreparedStatement getAllFacilities = null;
    PreparedStatement getAllAsets = null;
    public void createPhoenixConn() {
        try {
            // Connect to the database
            connection = DriverManager.getConnection("jdbc:phoenix:atlhashdn01.hashmap.net," +
                    "atlhashdn02.hashmap.net,atlhashdn03.hashmap.net:2181:/hbase-unsecure");
            connection.setAutoCommit(true);

        } catch (Exception e) {
            System.out.println("Exception is : " + e);
        }
    }
    public List<Facility> getAllFacilities(){
        List<Facility> list = new ArrayList<Facility>();
        try {
            getAllFacilities = connection.prepareStatement("select * from facility");
            ResultSet rs = getAllFacilities.executeQuery();
            while(rs.next()){
                Facility facility = new Facility();

                System.out.println("\n\n ****** here ***** \n\n");
                String name = rs.getString("name");
                facility.setFacilityName(name);

                String id = rs.getString("id");
                facility.setFacilityId(id);

                String type = rs.getString("type");
                facility.setFacilityType(type);

                String location = rs.getString("location");
                facility.setFacilityLocation(location);

                String attributes = rs.getString("attributes");
                facility.setFacilityAttribute(attributes);

                list.add(facility);
                System.out.println("\nfacility : " + facility + "\n");
            }

        }
        catch (Exception e){
            System.out.println("Exception : " + e );
        }
        return list;
    }

    public List<Asset> getAllAssets(){
        List<Asset> list = new ArrayList<Asset>();
        try {
            getAllAsets = connection.prepareStatement("select * from asset_test");
            ResultSet rs = getAllAsets.executeQuery();
            while(rs.next()){
                Asset asset = new Asset();

                String name = rs.getString("name");
                asset.setAssetName(name);

                String id = rs.getString("id");
                asset.setAssetId(id);

                String type = rs.getString("type");
                asset.setAssetType(type);

                String location = rs.getString("location");
                asset.setAssetLocation(location);

                String attributes = rs.getString("attributes");
                asset.setAssetAttributes(attributes);

                String facilityId = rs.getString("facilityId");
                asset.setFacilityId(facilityId);

                String parent = rs.getString("parent");
                asset.setAssetParent(parent);
                list.add(asset);
                System.out.println("Asset id : " + id);
            }

        }
        catch (Exception e){
            System.out.println("Exception : " + e );
        }
        return list;
    }

    public void closePhoenixConnection(){
        try {
            connection.close();
        }catch (Exception e){

        }
    }
    public static void main(String[] args){
        DataBaseService dataBaseService = new DataBaseService();
        dataBaseService.createPhoenixConn();
        /*List<Facility> facilityList = dataBaseService.getAllFacilities();
        for (Facility facility:facilityList
             ) {
            System.out.println("\n\nEach Facility " + facility.getFacilityLocation() + "\n\n");
        }*/

        List<Asset> assetList = dataBaseService.getAllAssets();
        for (Asset asset:assetList
                ) {
            System.out.println("\n\nEach Facility " + asset.getAssetLocation() + "\n\n");
        }
    }
}
