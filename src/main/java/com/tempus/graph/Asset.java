package com.tempus.graph;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.json.JSONObject;

/**
 * Created by himanshu on 29/8/17.
 */
public class Asset {
    final static Logger logger = Logger.getLogger(App.class);
    private String assetName;
    private String assetId;
    private String facilityId;
    private String assetType;
    private String assetLocation;
    private String assetParent;
    private String assetAttributes;

    public String getFacilityId() {
        return facilityId;
    }

    public String getAssetId() {
        return assetId;
    }

    public String getAssetName() {
        return assetName;
    }

    public String getAssetLocation() {
        return assetLocation;
    }

    public String getAssetParent() {
        return assetParent;
    }

    public String getAssetAttributes() {
        return assetAttributes;
    }

    public String getAssetType() {
        return assetType;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public void setFacilityId(String facilityId) {
        this.facilityId = facilityId;
    }

    public void setAssetLocation(String assetLocation) {
        this.assetLocation = assetLocation;
    }

    public void setAssetAttributes(String assetAttributes) {
        this.assetAttributes = assetAttributes;
    }

    public void setAssetName(String assetName) {
        this.assetName = assetName;
    }

    public void setAssetType(String assetType) {
        this.assetType = assetType;
    }

    public void setAssetParent(String assetParent) {
        this.assetParent = assetParent;
    }
    public Long getFacility(JanusGraph graph, String facilityId){
        Long ret = -1L;
        Vertex facility = null;
        GraphTraversalSource tr = graph.traversal();
        try {
            facility = tr.V().has("facilityID", facilityId).next();
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
        }catch (Exception ex){
            logger.error("Exception parent Asset does not exist : " + ex);
            return retParent;
        }
        String facilityStr = parentVertex.toString();
        String numberOnly= facilityStr.replaceAll("[^0-9]", "");
        retParent = Long.parseLong(numberOnly);
        return retParent;
    }

    /*public void addAssetVertex(JSONObject obj, JanusGraph graph){
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
            if(obj.has("loction"))
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
                logger.error("\n\n ####new asst #### : " + newAsset + "\n\n assetId : "+id + "\n");
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
    }*/
    public void addAssetVertex(JanusGraph graph){
        try {

            String id = "NA";
            String name = "NA";
            String location = "NA";
            String attributes = "NA";
            String parent = "NA";
            String facilityID = "NA";
            String type = "NA";

            /*if(obj.has("name"))
                name = obj.getString("name");
            if(obj.has("loction"))
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
            }*/

            id = this.getAssetId();
            name = this.getAssetName();
            location = this.getAssetLocation();
            facilityID = this.getFacilityId();
            type = this.getAssetType();
            parent = this.getAssetParent();
            attributes = this.getAssetAttributes();

            boolean assetAlreadyExists = this.checkAssetAlreadyExists(graph, id);
            if(assetAlreadyExists)
                return;
            Long facilityVertexLong = this.getFacility(graph, facilityID);

            if(facilityVertexLong != -1){
                logger.info("\n\n\n\n adding new asset : "+id+"\n\n\n\n ");
                JanusGraphTransaction tx = graph.newTransaction();
                Vertex facilityVertex = tx.getVertex(facilityVertexLong);
                Vertex newAsset = tx.addVertex(T.label,"asset","name",name,"assetId",id,"location",location,"facilityId",facilityID,
                        "attributes",attributes,"type",type,"parent",parent);
                logger.error("\n\n ####new asst #### : " + newAsset + "\n\n assetId : "+id + "\n");
                newAsset.addEdge("presentIn",facilityVertex);
                if(parent.contentEquals("NA") == false){
                    Long parentVertexLong = this.getParentVertex(graph,parent);
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
}
