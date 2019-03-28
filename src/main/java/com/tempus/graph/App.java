package com.tempus.graph;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.process.traversal.Order;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import org.json.*;

import static org.apache.http.protocol.HTTP.USER_AGENT;

/**
 * Program does Hello world! for demo purpose
 *
 */
public class App 
{
    final static Logger logger = Logger.getLogger(App.class);
    public void createGraphOfGods(JanusGraph graph, String mixedIndexName, boolean uniqueNameCompositeIndex){
        JanusGraphManagement mgmt = graph.openManagement();
        final PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        logger.debug("\n\n\nHMDC" + name + "\n\n\n");
        JanusGraphManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex("name", Vertex.class).addKey(name);
        if (uniqueNameCompositeIndex)
            nameIndexBuilder.unique();
        JanusGraphIndex namei = nameIndexBuilder.buildCompositeIndex();
        mgmt.setConsistency(namei, ConsistencyModifier.LOCK);
        final PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
        if (null != mixedIndexName)
            mgmt.buildIndex("vertices", Vertex.class).addKey(age).buildMixedIndex(mixedIndexName);

        final PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        final PropertyKey reason = mgmt.makePropertyKey("reason").dataType(String.class).make();
        final PropertyKey place = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
        if (null != mixedIndexName)
            mgmt.buildIndex("edges", Edge.class).addKey(reason).addKey(place).buildMixedIndex(mixedIndexName);

        mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel battled = mgmt.makeEdgeLabel("battled").signature(time).make();
        mgmt.buildEdgeIndex(battled, "battlesByTime", Direction.BOTH, Order.decr, time);
        mgmt.makeEdgeLabel("lives").signature(reason).make();
        mgmt.makeEdgeLabel("pet").make();
        mgmt.makeEdgeLabel("brother").make();

        mgmt.makeVertexLabel("titan").make();
        mgmt.makeVertexLabel("location").make();
        mgmt.makeVertexLabel("god").make();
        mgmt.makeVertexLabel("demigod").make();
        mgmt.makeVertexLabel("human").make();
        mgmt.makeVertexLabel("monster").make();

        mgmt.commit();

        JanusGraphTransaction tx = graph.newTransaction();
        // vertices

        Vertex saturn = tx.addVertex(T.label, "titan", "name", "saturn", "age", 10000);
        Vertex sky = tx.addVertex(T.label, "location", "name", "sky");
        Vertex sea = tx.addVertex(T.label, "location", "name", "sea");
        Vertex jupiter = tx.addVertex(T.label, "god", "name", "jupiter", "age", 5000);
        Vertex neptune = tx.addVertex(T.label, "god", "name", "neptune", "age", 4500);
        Vertex hercules = tx.addVertex(T.label, "demigod", "name", "hercules", "age", 30);
        Vertex alcmene = tx.addVertex(T.label, "human", "name", "alcmene", "age", 45);
        Vertex pluto = tx.addVertex(T.label, "god", "name", "pluto", "age", 4000);
        Vertex nemean = tx.addVertex(T.label, "monster", "name", "nemean");
        Vertex hydra = tx.addVertex(T.label, "monster", "name", "hydra");
        Vertex cerberus = tx.addVertex(T.label, "monster", "name", "cerberus");
        Vertex tartarus = tx.addVertex(T.label, "location", "name", "tartarus");

        // edges

        jupiter.addEdge("father", saturn);
        jupiter.addEdge("lives", sky, "reason", "loves fresh breezes");
        jupiter.addEdge("brother", neptune);
        jupiter.addEdge("brother", pluto);

        neptune.addEdge("lives", sea).property("reason", "loves waves");
        neptune.addEdge("brother", jupiter);
        neptune.addEdge("brother", pluto);

        hercules.addEdge("father", jupiter);
        hercules.addEdge("mother", alcmene);
        hercules.addEdge("battled", nemean, "time", 1, "place", Geoshape.point(38.1f, 23.7f));
        hercules.addEdge("battled", hydra, "time", 2, "place", Geoshape.point(37.7f, 23.9f));
        hercules.addEdge("battled", cerberus, "time", 12, "place", Geoshape.point(39f, 22f));

        pluto.addEdge("brother", jupiter);
        pluto.addEdge("brother", neptune);
        pluto.addEdge("lives", tartarus, "reason", "no fear of death");
        pluto.addEdge("pet", cerberus);

        cerberus.addEdge("lives", tartarus);

        // commit the transaction to disk
        tx.commit();
    }

    public JanusGraph createFacilityAndAssetGraphSchema(JanusGraph graph, boolean uniqueNameCompositeIndex){
        JanusGraphManagement mgmt = graph.openManagement();
        // property
        //final PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        final PropertyKey facilityId = mgmt.makePropertyKey("facilityID").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        final PropertyKey assetId = mgmt.makePropertyKey("assetId").dataType(String.class).cardinality(Cardinality.SINGLE).make();

        JanusGraphManagement.IndexBuilder facilityIdIndexBuilder = mgmt.buildIndex("ByFacilityID", Vertex.class).addKey(facilityId);
        if (uniqueNameCompositeIndex)
            facilityIdIndexBuilder.unique();
        JanusGraphIndex facilityIdi = facilityIdIndexBuilder.buildCompositeIndex();
        mgmt.setConsistency(facilityIdi, ConsistencyModifier.LOCK);

        JanusGraphManagement.IndexBuilder assetIdIndexBuilder = mgmt.buildIndex("ByAssetId", Vertex.class).addKey(assetId);
        if (uniqueNameCompositeIndex)
            assetIdIndexBuilder.unique();
        JanusGraphIndex assetIdi = assetIdIndexBuilder.buildCompositeIndex();
        mgmt.setConsistency(assetIdi, ConsistencyModifier.LOCK);


        //mgmt.makePropertyKey("assetId").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makePropertyKey("location").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("attributes").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("type").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("parent").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("facilityId").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        // Edges
        mgmt.makeEdgeLabel("presentIn").multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.makeEdgeLabel("childOf").multiplicity(Multiplicity.MANY2ONE).make();
        // Label parentVetex
        mgmt.makeVertexLabel("StartPoint").make();
        VertexLabel startPointLabel = mgmt.getVertexLabel("StartPoint");
        mgmt.buildIndex("ByNameAndLabel", Vertex.class).addKey(name).indexOnly(startPointLabel).buildCompositeIndex();
        // Labels facilty
        mgmt.makeVertexLabel("facility").make();
        mgmt.makeVertexLabel("asset").make();
        mgmt.commit();

        JanusGraphTransaction tx = graph.newTransaction();
        Vertex start = tx.addVertex(T.label, "StartPoint","name","Start");
        Vertex facility =tx.addVertex(T.label,"facility","name","facility1","facilityID","2d45a1a8-5c2f-43a0-b1cc-0b9f3cd2ceab");
        /*Vertex asset = tx.addVertex(T.label,"asset","name","gremlin2","facilityId","2d45a1a8-5c2f-43a0-b1cc-0b9f3cd2ceab"
                ,"assetId","29813ab7-6594-4aba-95dc-5dbec69654ac");
        //Vertex satrt = tx.getVertex(1);
        facility.addEdge("contains",asset);*/
        tx.commit();
        return graph;
    }

    public String getResource(String resource) throws Exception {

        String url = "http://192.166.4.40:9003/api/" + resource;

        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        int responseCode = con.getResponseCode();
        logger.debug("\nSending 'GET' request to URL : " + url);
        logger.debug("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //print result
        logger.debug(response.toString());
        return response.toString();
    }

    public Long getFacility(JanusGraph graph){
        Long ret = -1L;
        Vertex facility = null;
        GraphTraversalSource tr = graph.traversal();
        try {
            facility = tr.V().has("facilityID", "2d45a1a8-5c2f-43a0-b1cc-0b9f3cd2ceab").next();
            logger.error("\n\n ******* facility : ***** \n\n" + facility + "\n\n");
        }catch (Exception ex){
            logger.error("Exception facility does not exist : " + ex);
            return ret;
        }
        String facilityStr = facility.toString();
        String numberOnly= facilityStr.replaceAll("[^0-9]", "");
        ret = Long.parseLong(numberOnly);
        return ret;
    }

    public boolean checkAssetAlreadyExists(JanusGraph graph, String assetId){
        GraphTraversalSource tr = graph.traversal();
        Vertex asset = null;
        try {
            asset = tr.V().has("assetId", assetId).next();
            logger.debug("\n\n ******* asset: ***** \n\n" + asset + "\n\n");
        }catch (Exception ex){
            logger.error("Asset does not exist create new one : " + ex);
            return false;
        }
        return true;
    }

    public Long getParentVertex(JanusGraph graph, String parentId){
        long retParent = -1;
        Vertex parentVertex = null;
        GraphTraversalSource tr = graph.traversal();
        try {
            parentVertex = tr.V().has("assetId", parentId).next();
            logger.debug("\n\n ******* parent : ***** \n\n" + parentVertex + "\n\n");
        }catch (Exception ex){
            logger.error("Exception parent Asset does not exist : " + ex);
            return retParent;
        }
        String facilityStr = parentVertex.toString();
        String numberOnly= facilityStr.replaceAll("[^0-9]", "");
        retParent = Long.parseLong(numberOnly);
        return retParent;
    }

    public void addVertex(JSONObject obj, JanusGraph graph){
        try {

            String id = "NA";
            String name = "NA";
            String location = "NA";
            String attributes = "NA";
            String parent = "NA";
            String facilityID = "NA";
            String type = "NA";

            if(obj.has("name"))
                name = obj.getString("name");
            if(obj.has("location"))
                location = obj.getString("location");
            if(obj.has("attributes"))
                attributes = obj.getString("attributes");
            if(obj.has("parent"))
                parent = obj.getString("parent");
            if(obj.has("type"))
                type = obj.getString("type");
            if(obj.has("id")) {
                id = obj.getString("id");
            }
            else {
                logger.error("AssetId not present. It is mandatory. Returning back!");
                return;
            }
            if(obj.has("facilityId")) {
                facilityID = obj.getString("facilityId");
            }
            else {
                logger.error("facilityId not present. It is mandatory. Returning back!");
                return;
            }
            boolean assetAlreadyExists = checkAssetAlreadyExists(graph, id);
            if(assetAlreadyExists)
                return;
            Long facilityVertexLong = getFacility(graph);

            if(facilityVertexLong != -1){
                logger.info("\n\n\n\n adding new asset : "+id+"\n\n\n\n ");
                JanusGraphTransaction tx = graph.newTransaction();
                Vertex facilityVertex = tx.getVertex(facilityVertexLong);
                Vertex newAsset = tx.addVertex(T.label,"asset","name",name,"assetId",id,"location",location,"facilityId",facilityID,
                        "attributes",attributes,"type",type,"parent",parent);
                logger.error("\n\n ####new asst#### : " + newAsset + "\n\n assetId : "+id + "\n");
                newAsset.addEdge("presentIn",facilityVertex);
                if(parent.contentEquals("NA") == false){
                    Long parentVertexLong = getParentVertex(graph,parent);
                    if (parentVertexLong != -1){
                        Vertex parentVertex = tx.getVertex(parentVertexLong);
                        newAsset.addEdge("childOf",parentVertex);
                    }
                }
                tx.commit();
            }

            logger.debug("associated is : " + type);
        }
        catch (Exception e){
            logger.error("Exception is : " + e);
        }
    }

    public void createGraph(JanusGraph graph, String jsonStr){
        try {
            String defaultStr = "[{\n" +
                    "    \"name\": \"Pump 1\",\n" +
                    "    \"id\": \"63aa1d27-7693-4dfd-97a4-61d5a37e8cb2\",\n" +
                    "    \"facilityId\": \"2d45a1a8-5c2f-43a0-b1cc-0b9f3cd2ceab\",\n" +
                    "    \"type\": \"Pump\",\n" +
                    "    \"location\": \"29.7604267, -95.3698028\",\n" +
                    "    \"attributes\": {\n" +
                    "      \"creationDate\": \"2017-08-25\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"name\": \"Tank Child\",\n" +
                    "    \"id\": \"14015bf9-ae7e-4b81-b751-743af9f69af8\",\n" +
                    "    \"facilityId\": \"2d45a1a8-5c2f-43a0-b1cc-0b9f3cd2ceab\",\n" +
                    "    \"parent\": \"29813ab7-6594-4aba-95dc-5dbec69654ac\",\n" +
                    "    \"type\": \"Tank\",\n" +
                    "    \"location\": \"29.7604267, -95.3698028\"\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"name\": \"Tank A\",\n" +
                    "    \"id\": \"29813ab7-6594-4aba-95dc-5dbec69654ad\",\n" +
                    "    \"facilityId\": \"2d45a1a8-5c2f-43a0-b1cc-0b9f3cd2ceab\",\n" +
                    "    \"type\": \"Tank\",\n" +
                    "    \"location\": \"29.7604267, -95.3698028\",\n" +
                    "    \"associated\": [\n" +
                    "      {\n" +
                    "        \"name\": \"Tank Child\",\n" +
                    "        \"id\": \"14015bf9-ae7e-4b81-b751-743af9f69af0\"\n" +
                    "      }\n" +
                    "    ],\n" +
                    "    \"attributes\": {\n" +
                    "      \"creationDate\": \"2017-08-28\"\n" +
                    "    }\n" +
                    "  },\n" +
                    "  {\n" +
                    "    \"name\": \"Pump 2\",\n" +
                    "    \"id\": \"a8444ca4-c75a-4042-ab94-2bcc0084c735\",\n" +
                    "    \"facilityId\": \"2d45a1a8-5c2f-43a0-b1cc-0b9f3cd2ceam\",\n" +
                    "    \"type\": \"Tank\",\n" +
                    "    \"attributes\": {\n" +
                    "      \"creationDate\": \"2017-08-28\"\n" +
                    "    }\n" +
                    "  }]";
            JSONArray jsonArray = new JSONArray(defaultStr);
            logger.debug("Json obj: " + jsonArray);
            for(int i = 0; i < jsonArray.length(); i++)
            {
                JSONObject obj = jsonArray.getJSONObject(i);
                logger.debug("obj("+i+") = " + obj);
                addVertex(obj, graph);
            }

        }catch (Exception e){
            logger.debug("Json Exception is : " + e);
        }
    }

    public static void main( String[] args )
    {
        logger.debug( "Hello World!" );
        JanusGraph graph = JanusGraphFactory.open("/home/himanshu/Documents/janusgraph-0.1.1-hadoop2/conf/janusgraph-hbase.properties");
        App app = new App();
        String jsonStr = "test";
        try {
            //jsonStr = app.getResource("assets");
            graph = app.createFacilityAndAssetGraphSchema(graph,true);
            //app.createGraph(graph, jsonStr);
            /*GraphTraversalSource tr = graph.traversal();

            String vr = tr.V().has("assetId", "assetId").toString();
            logger.error("vr is : " + vr);
            tr.close();*/
        }
        catch (Exception e){
            logger.debug("Error : " + e);
        }
        //app.createGraphOfGods(graph, "search", true);
        //app.createFacilityAndAssetGraphSchema(graph,true);
        /*DataBaseService dataBaseService = new DataBaseService();
        //dataBaseService.createPhoenixConn();
        List<Facility> facilityList =  dataBaseService.getAllFacilities();
        List<Asset> assetList = dataBaseService.getAllAssets();

        logger.debug("Facility List : " + facilityList);
        logger.debug("Asset List : " + assetList);*/

        //JanusGraph graph = JanusGraphFactory.build().set("storage.backend", "hbase").open();
        //JanusGraphManagement mgmt = janusGraph.openManagement();
        graph.close();
    }
}
