package com.tempus.graph;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by himanshu on 29/8/17.
 */
public class Facility {
    final static Logger logger = Logger.getLogger(App.class);
    private String facilityId;
    private String facilityName;
    private String facilityType;
    private String facilityAttribute;
    private String facilityLocation;

    public String getFacilityAttribute() {
        return facilityAttribute;
    }

    public String getFacilityId() {
        return facilityId;
    }

    public String getFacilityName() {
        return facilityName;
    }

    public String getFacilityLocation() {
        return facilityLocation;
    }

    public String getFacilityType() {
        return facilityType;
    }

    public void setFacilityId(String facilityId) {
        this.facilityId = facilityId;
    }

    public void setFacilityAttribute(String facilityAttribute) {
        this.facilityAttribute = facilityAttribute;
    }

    public void setFacilityName(String facilityName) {
        this.facilityName = facilityName;
    }

    public void setFacilityType(String facilityType) {
        this.facilityType = facilityType;
    }

    public void setFacilityLocation(String facilityLocation) {
        this.facilityLocation = facilityLocation;
    }

    public boolean checkFacilityAlreadyExists(JanusGraph graph, String facilityId){
        GraphTraversalSource tr = graph.traversal();
        Vertex facility = null;
        boolean ret = true;
        try {
            facility = tr.V().has("facilityID", facilityId).next();
            logger.debug("\n\n ******* Facility: ***** \n\n" + facility + "\n\n");
        }catch (Exception ex){
            logger.error("Facility does not exist create new one : " + ex);
            ret  = false;
        }
        return ret;
    }

    public Long getStartVertex(JanusGraph graph){
        Long ret = -1L;
        Vertex startVertex = null;
        GraphTraversalSource tr = graph.traversal();
        try {
            startVertex =  tr.V().has("name","Start").next();
            logger.error("\n\n ******* Start vertex : ***** \n\n" + startVertex + "\n\n");
        }catch (Exception ex){
            logger.error("Exception facility does not exist : " + ex);
            return ret;
        }
        String startVertexStr = startVertex.toString();
        String numberOnly= startVertexStr.replaceAll("[^0-9]", "");
        ret = Long.parseLong(numberOnly);
        return ret;
    }

    /*public void addFacilityVertex(JSONObject obj, JanusGraph graph){
        try {

            String id = "NA";
            String name = "NA";
            String location = "NA";
            String attributes = "NA";
            String type = "NA";

            if(obj.has("name"))
                name = obj.getString("name");
            if(obj.has("location"))
                location = obj.getString("location");
            if(obj.has("attributes"))
                attributes = obj.getString("attributes");
            if(obj.has("type"))
                type = obj.getString("type");
            if(obj.has("id")) {
                id = obj.getString("id");
            }
            else {
                logger.error("AssetId not present. It is mandatory. Returning back!");
                return;
            }

            boolean assetAlreadyExists = checkFacilityAlreadyExists(graph, id);
            if(assetAlreadyExists)
                return;

            Long startVertexLong = getStartVertex(graph);
            logger.debug("Start vertex is : " + startVertexLong);

            if(startVertexLong != -1){
                logger.info("\n\n\n\n adding new facility : "+id+"\n\n\n\n ");
                JanusGraphTransaction tx = graph.newTransaction();
                Vertex startVertex = tx.getVertex(startVertexLong);
                Vertex newFacility = tx.addVertex(T.label,"facility","name",name,"facilityID",id,"location",location,
                        "attributes",attributes,"type",type);
                logger.error("\n\n ####new facility #### : " + newFacility + "\n\n facilityID : "+id + "\n");
                newFacility.addEdge("presentIn",startVertex);
                tx.commit();
            }

            logger.debug("associated is : " + type);
        }
        catch (Exception e){
            logger.error("Exception is : " + e);
        }
    }*/

    public void addFacilityVertex(JanusGraph graph){
        try {

            String id = "NA";
            String name = "NA";
            String location = "NA";
            String attributes = "NA";
            String type = "NA";

            /*if(obj.has("name"))
                name = obj.getString("name");
            if(obj.has("location"))
                location = obj.getString("location");
            if(obj.has("attributes"))
                attributes = obj.getString("attributes");
            if(obj.has("type"))
                type = obj.getString("type");
            if(obj.has("id")) {
                id = obj.getString("id");
            }
            else {
                logger.error("AssetId not present. It is mandatory. Returning back!");
                return;
            }

            boolean assetAlreadyExists = checkFacilityAlreadyExists(graph, id);
            if(assetAlreadyExists)
                return;
            */
            // Get all facilities.
            id = this.getFacilityId();
            name = this.getFacilityName();
            location = this.getFacilityLocation();
            type = this.getFacilityType();
            attributes = this.getFacilityAttribute();
            Long startVertexLong = this.getStartVertex(graph);

            logger.debug("Start vertex is : " + startVertexLong);

            if(startVertexLong != -1){
                logger.info("\n\n\n\n adding new facility : "+id+"\n\n\n\n ");
                JanusGraphTransaction tx = graph.newTransaction();
                Vertex startVertex = tx.getVertex(startVertexLong);
                Vertex newFacility = tx.addVertex(T.label,"facility","name",name,"facilityID",id,"location",location,
                        "attributes",attributes,"type",type);
                logger.error("\n\n ####new facility #### : " + newFacility + "\n\n facilityID : "+id + "\n");
                newFacility.addEdge("presentIn",startVertex);
                tx.commit();
            }

            logger.debug("associated is : " + type);
        }
        catch (Exception e){
            logger.error("Exception is : " + e);
        }
    }

    public static void main( String[] args )
    {
        JanusGraph graph = JanusGraphFactory.open("/home/himanshu/Documents/janusgraph-0.1.1-hadoop2/conf/janusgraph-hbase.properties");
        Facility facilityObj = new Facility();
        String defaultStr = "[\n" +
                "  {\n" +
                "    \"name\": \"Houston\",\n" +
                "    \"id\": \"2d45a1a8-5c2f-43a0-b1cc-0b9f3cd2ceab\",\n" +
                "    \"type\": \"Substation A\",\n" +
                "    \"location\": \"29.7604267, -95.3698028\"\n" +
                "  },\n" +
                "  {\n" +
                "    \"name\": \"cc8b1d11-0b8c-455c-a229-b312bf6764ed\",\n" +
                "    \"id\": \"37457720-8c38-4fa4-9d5d-01f0652e14f6\",\n" +
                "    \"type\": \"Houston Substation 1\",\n" +
                "    \"location\": \"Houston\"\n" +
                "  }\n" +
                "]";
        try {
            JSONArray jsonArray = new JSONArray(defaultStr);
            logger.debug("Json obj: " + jsonArray);
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                logger.debug("obj(" + i + ") = " + obj);
                //facilityObj.addFacilityVertex(obj, graph);
            }
        }
        catch (Exception e){
            logger.error("Exception : " + e);
        }
        graph.close();
    }
}
