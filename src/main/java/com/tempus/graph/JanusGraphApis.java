package com.tempus.graph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by himanshu on 1/9/17.
 */
public class JanusGraphApis {

    public JanusGraph createFacilityAndAssetGraphSchema(JanusGraph graph, boolean uniqueNameCompositeIndex){
        JanusGraphManagement mgmt = graph.openManagement();
        // property

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
        tx.addVertex(T.label, "StartPoint","name","Start");
        tx.commit();
        return graph;
    }

    public List<Facility> getAllfacilities(JanusGraph graph){

        List<Facility> facilityList = new ArrayList<Facility>();
        GraphTraversalSource tr = graph.traversal();
        GraphTraversal<Vertex,List<Vertex>> x = tr.V().has("name","Start").in("presentIn").fold();

        List<Vertex> vertexList = x.next();
        for (Vertex vertex : vertexList) {
            Facility facility = new Facility();
            Object facilityId = vertex.property("facilityID").value();
            Object facilityName = vertex.property("name").value();
            Object facilityType = vertex.property("type").value();
            Object facilityAttributes = vertex.property("attributes").value();
            Object facilityLocation = vertex.property("location").value();

            //facility.setFacilityId(facilityId);
            facility.setFacilityId(facilityId.toString());
            facility.setFacilityName(facilityName.toString());
            facility.setFacilityType(facilityType.toString());
            facility.setFacilityAttribute(facilityAttributes.toString());
            facility.setFacilityLocation(facilityLocation.toString());

            facilityList.add(facility);
        }
        return facilityList;
    }

    public Facility getfacilityById(JanusGraph graph, String facilityId){
        GraphTraversalSource tr = graph.traversal();
        Vertex vertex =  tr.V().has("facilityID",facilityId).next();


        Facility facility = new Facility();
        Object facilityName = vertex.property("name").value();
        Object facilityType = vertex.property("type").value();
        Object facilityAttributes = vertex.property("attributes").value();
        Object facilityLocation = vertex.property("location").value();

        //facility.setFacilityId(facilityId);
        facility.setFacilityId(facilityId);
        facility.setFacilityName(facilityName.toString());
        facility.setFacilityType(facilityType.toString());
        facility.setFacilityAttribute(facilityAttributes.toString());
        facility.setFacilityLocation(facilityLocation.toString());

        return facility;
    }

    public Asset getAssetById(JanusGraph graph, String assetId){
        Asset asset = new Asset();
        GraphTraversalSource tr = graph.traversal();
        Vertex vertex = tr.V().has("assetId",assetId).next();

        Object assetName = vertex.property("name").value();
        Object assetType = vertex.property("type").value();
        Object assetAttributes = vertex.property("attributes").value();
        Object assetLocation = vertex.property("location").value();
        Object assetParent = vertex.property("parent").value();

        //asset.setFacilityId(facilityId);
        asset.setAssetId(assetId);
        asset.setAssetName(assetName.toString());
        asset.setAssetType(assetType.toString());
        asset.setAssetAttributes(assetAttributes.toString());
        asset.setAssetLocation(assetLocation.toString());
        asset.setAssetParent(assetParent.toString());

        return asset;
    }
    
    public List<Asset> getAllAssets(JanusGraph graph, String facilityId){

        List<Asset> assetList = new ArrayList<Asset>();
        GraphTraversalSource tr = graph.traversal();
        GraphTraversal<Vertex,List<Vertex>> x = tr.V().has("facilityID",facilityId).in("presentIn").fold();

        List<Vertex> vertexList=x.next();
        for (Vertex vertex : vertexList
             ) {
            Asset asset = new Asset();
            Object assetId = vertex.property("assetId").value();
            Object assetName = vertex.property("name").value();
            Object assetType = vertex.property("type").value();
            Object assetAttributes = vertex.property("attributes").value();
            Object assetLocation = vertex.property("location").value();
            Object assetParent = vertex.property("parent").value();

            //asset.setFacilityId(facilityId);
            asset.setAssetId(assetId.toString());
            asset.setAssetName(assetName.toString());
            asset.setAssetType(assetType.toString());
            asset.setAssetAttributes(assetAttributes.toString());
            asset.setAssetLocation(assetLocation.toString());
            asset.setAssetParent(assetParent.toString());

            assetList.add(asset);
        }
        return assetList;
    }

    public static void main(String[] args){
        JanusGraph graph = JanusGraphFactory.open("/home/himanshu/Documents/janusgraph-0.1.1-hadoop2/conf/janusgraph-hbase.properties");
        JanusGraphApis janusGraphApis = new JanusGraphApis();
        //List<Asset> assetList = janusGraphApis.getAllAssets(graph, "2d45a1a8-5c2f-43a0-b1cc-0b9f3cd2ceab");
        //System.out.println("Asset List : " + assetList);
        List<Asset> assetList = janusGraphApis.getAllAssets(graph, "2d45a1a8-5c2f-43a0-b1cc-0b9f3cd2ceab");
        for (Asset asset :assetList
             ) {
            System.out.println("Asset id : " + asset.getAssetId() + "Asset Name : " + asset.getAssetName());
        }
        /*List<Facility> facilityList = janusGraphApis.getAllfacilities(graph);
        for (Facility facility :facilityList
                ) {
            System.out.println("Asset id : " + facility.getFacilityId() + "Asset Name : " + facility.getFacilityName());
        }*/
        graph.close();
    }
}
