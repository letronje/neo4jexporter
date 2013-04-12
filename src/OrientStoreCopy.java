import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;

import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.batch.OrientBatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;

public class OrientStoreCopy {

	private static PrintWriter logs;

	@SuppressWarnings("unchecked")
	public static Map<String, String> config() {
		return (Map) MapUtil.map("neostore.nodestore.db.mapped_memory", "100M",
				"neostore.relationshipstore.db.mapped_memory", "500M",
				"neostore.propertystore.db.mapped_memory", "300M",
				"neostore.propertystore.db.strings.mapped_memory", "1G",
				"neostore.propertystore.db.arrays.mapped_memory", "300M",
				"neostore.propertystore.db.index.keys.mapped_memory", "100M",
				"neostore.propertystore.db.index.mapped_memory", "100M",
				"cache_type", "weak");
	}

	public static void main(String[] args) throws Exception {
		//OrientGraph ograph = null;
		OrientBatchGraph obgraph = null;
		BatchGraph batchGraph = null;
		GraphDatabaseService sourceDb = null;
		
		try{
			//ograph = new OrientGraph("local:/home/letronje/workspace/tools/orient.db");
			obgraph = new OrientBatchGraph("local:/home/letronje/workspace/tools/orient.db");
			
			//ograph.getRawGraph().declareIntent(new OIntentMassiveInsert());
			obgraph.getRawGraph().declareIntent(new OIntentMassiveInsert());
			
			//BatchGraph batchGraph = new BatchGraph(obgraph, VertexIDType.NUMBER, BatchGraph.DEFAULT_BUFFER_SIZE);
			batchGraph = BatchGraph.wrap(obgraph, BatchGraph.DEFAULT_BUFFER_SIZE);
			
			String sourceDir = "/home/letronje/workspace/tools/neo4j-community-1.8.2/data/graph.db";
			
			sourceDb = new EmbeddedGraphDatabase(sourceDir, config());
			
			//Index<Vertex> vertices = graph.createIndex("vertices", Vertex.class);
						
			copyVertices(sourceDb, batchGraph);
			copyEdges(sourceDb, batchGraph);
		}
		finally{
			/*if(ograph != null ){
				ograph.shutdown();
			}*/
			if(batchGraph != null ){
				batchGraph.shutdown();
			}
			if(sourceDb != null ){
				sourceDb.shutdown();
			}
		}
	}

	private static Iterable<Relationship> getOutgoingRelationships(Node node) {
		try {
			return node.getRelationships(Direction.OUTGOING);
		} catch (InvalidRecordException ire) {
			return Collections.emptyList();
		}
	}
	
	private static void copyEdges(GraphDatabaseService sourceDb,
			BatchGraph batchGraph) {
		long time = System.currentTimeMillis();
		long count = 0;
		long edgetimes = 0;
		long vertextimes = 0;
		
		for (Node node : sourceDb.getAllNodes()) {
			for (Relationship rel : getOutgoingRelationships(node)) {
				long startNodeId = rel.getStartNode().getId();
				long endNodeId = rel.getEndNode().getId();
				
				final RelationshipType type = rel.getType();
				
				long ts = System.currentTimeMillis();
				
				
				Vertex startVertex = batchGraph.getVertex(startNodeId);
				Vertex endVertex = batchGraph.getVertex(endNodeId);
				
				long te = System.currentTimeMillis();
				
				vertextimes += (te-ts);
				
				ts = te;
				
				Edge edge = batchGraph.addEdge(null, 
						startVertex,
						endVertex,
						type.name());
				
				te = System.currentTimeMillis();
				
				edgetimes += (te-ts);
				
				Iterable<String> keys = rel.getPropertyKeys();
				for(String key: keys){
					Object value = rel.getProperty(key); 
					edge.setProperty(key, value);
				}
				
				count++;
				if (count % 100 == 0)
					System.out.print(".");
				if (count % 10000 == 0){
					System.out.println(" " + count);
					System.out.println(edgetimes);
					System.out.println(vertextimes);
					edgetimes = 0;
					vertextimes = 0;
				}
			}
		}
		System.out.println("\n copying of " + count + " relationships took "
				+ (System.currentTimeMillis() - time) + " ms.");
	}

	private static void copyVertices(GraphDatabaseService sourceDb,
			BatchGraph batchGraph) {
		long time = System.currentTimeMillis();
		int count = 0;
		
		for (Node node : sourceDb.getAllNodes()) {
			
			Vertex vertex = batchGraph.addVertex(node.getId());
			long t1 = System.currentTimeMillis();
			//vertices.put("vertices", node.getId(), vertex);
			//long t2 = System.currentTimeMillis();
			//System.out.println(t2-t1);
			
			Iterable<String> keys = node.getPropertyKeys();
			for(String key: keys){
				if(key.equals("id")){
					continue;
				}
				Object value = node.getProperty(key); 
				vertex.setProperty(key, value);
			}
			count ++;
			if (count % 100 == 0)
				System.out.print(".");
			if (count % 10000 == 0) {
				//graph.stopTransaction(Conclusion.SUCCESS);
				System.out.println(" " + count);
				//break;
			}
			//if(count % 200000 == 0){
				//break;
			//}
		}
		System.out.println("\n copying of " + count + " nodes took "
				+ (System.currentTimeMillis() - time) + " ms.");
	}

	
}