package com.tempus.graph;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.log4j.Logger;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * Created by himanshu on 4/9/17.
 */
public class GraphController {
    final static Logger logger = Logger.getLogger(GraphController.class);
    Properties configProp;
    public void loadConfigProperties(){
        logger.debug("In function loadConfigProperties()");
        InputStream input = null;

        try {

            String filename = "config.properties";
            input = getClass().getClassLoader().getResourceAsStream(filename);
            if(input==null){
                logger.error("Sorry, unable to find " + filename);
                return;
            }

            //load a properties file from class path, inside static method
            configProp.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally{
            if(input!=null){
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public boolean IsSchemaCreationNeeded(){
        boolean schemaCreationNeeded = false;
        if(configProp.getProperty("SCHEMA_CREATION").contentEquals("true"))
            schemaCreationNeeded = true;
        return schemaCreationNeeded;
    }

    public  boolean getAddFacilityStatus(){
        boolean addFacilty = false;
        if(configProp.getProperty("ADD_FACILITIES").contentEquals("true"))
            addFacilty = true;
        return addFacilty;
    }

    public  boolean getAddAssetStatus(){
        boolean addFacilty = false;
        if(configProp.getProperty("ADD_ASSETS").contentEquals("true"))
            addFacilty = true;
        return addFacilty;
    }

    // Just for listing all facilities and Assets.
    public void traverseGraph(JanusGraph graph, JanusGraphApis janusGraphApis){
        List<Facility> facilityList = janusGraphApis.getAllfacilities(graph);
        for (Facility facility : facilityList) {
            logger.info("Facility : " + facility);
            List<Asset> assetList = janusGraphApis.getAllAssets(graph,facility.getFacilityId());
            for (Asset asset : assetList
                 ) {
                System.out.println("Retrieved assets  : " + asset.getAssetId());
            }
        }
    }

    public static void main(String[] args){
        // Create graph schema
        // Add Facility
        // Add Asset
        // Traverse graph
        JanusGraph graph = JanusGraphFactory.open("/home/jkapadnis/himanshu/janusgraph-0.1.1-hadoop2/conf/janusgraph-hbase-solr.properties");
        GraphController graphController = new GraphController();
        JanusGraphApis janusGraphApis = new JanusGraphApis();
        //if(graphController.IsSchemaCreationNeeded()){
            //boolean compositeIndex = true;
            //janusGraphApis.createFacilityAndAssetGraphSchema(graph, compositeIndex);
        //}

        //if(graphController.getAddFacilityStatus()){
        if(args[0].contentEquals("add_facility"))
        {
            //System.out.println("Here " + args[0]);
            DataBaseService dataBaseService = new DataBaseService();
            dataBaseService.createPhoenixConn();

            List <Facility> facilityList= dataBaseService.getAllFacilities();
            for (Facility facility : facilityList) {
                facility.addFacilityVertex(graph);
            }
            dataBaseService.closePhoenixConnection();
        }

        //if(graphController.getAddAssetStatus()){
        else if(args[0].contentEquals("add_asset")){
            DataBaseService dataBaseService = new DataBaseService();
            dataBaseService.createPhoenixConn();

            List <Asset> assetList= dataBaseService.getAllAssets();
            for (Asset asset : assetList) {
                System.out.println("Asset is : " + asset.getAssetId());
                asset.addAssetVertex(graph);
            }
            dataBaseService.closePhoenixConnection();
        }

        else if(args[0].contentEquals("get_all_facilities")){
            graphController.traverseGraph(graph, janusGraphApis);
        }


        graph.close();
    }
}
