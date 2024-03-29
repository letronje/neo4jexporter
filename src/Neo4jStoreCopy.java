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

public class Neo4jStoreCopy {

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
		if (args.length < 2) {
			System.err
					.println("Usage: StoryCopy source target [rel,types,to,ignore] [properties,to,ignore]");
			return;
		}
		String sourceDir = args[0];
		String targetDir = args[1];
		Set<String> ignoreRelTypes = splitOptionIfExists(args, 2);
		Set<String> ignoreProperties = splitOptionIfExists(args, 3);
		System.out
				.printf("Copying from %s to %s ingoring rel-types %s ignoring properties %s %n",
						sourceDir, targetDir, ignoreRelTypes, ignoreProperties);
		copyStore(sourceDir, targetDir, ignoreRelTypes, ignoreProperties);
	}

	private static Set<String> splitOptionIfExists(String[] args,
			final int index) {
		if (args.length <= index)
			return emptySet();
		return new HashSet<String>(asList(args[index].toLowerCase().split(",")));
	}

	private static void copyStore(String sourceDir, String targetDir,
			Set<String> ignoreRelTypes, Set<String> ignoreProperties)
			throws Exception {
		final File target = new File(targetDir);
		final File source = new File(sourceDir);
		if (target.exists())
			throw new IllegalArgumentException(
					"Target Directory already exists " + target);
		if (!source.exists())
			throw new IllegalArgumentException(
					"Source Database does not exist " + source);

		BatchInserter targetDb = new BatchInserterImpl(
				target.getAbsolutePath(), config());
		GraphDatabaseService sourceDb = new EmbeddedGraphDatabase(sourceDir,
				config());
		logs = new PrintWriter(new FileWriter(
				new File(target, "store-copy.log")));

		copyNodes(sourceDb, targetDb, ignoreProperties);
		copyRelationships(sourceDb, targetDb, ignoreRelTypes, ignoreProperties);

		targetDb.shutdown();
		sourceDb.shutdown();
		logs.close();
		copyIndex(source, target);
	}

	private static void copyIndex(File source, File target) throws IOException {
		final File indexFile = new File(source, "index.db");
		if (indexFile.exists()) {
			FileUtils.copyFile(indexFile, new File(target, "index.db"));
		}
		final File indexDir = new File(source, "index");
		if (indexDir.exists()) {
			FileUtils.copyDirectory(indexDir, new File(target, "index"));
		}
	}

	private static void copyRelationships(GraphDatabaseService sourceDb,
			BatchInserter targetDb, Set<String> ignoreRelTypes,
			Set<String> ignoreProperties) {
		long time = System.currentTimeMillis();
		int count = 0;
		for (Node node : sourceDb.getAllNodes()) {
			for (Relationship rel : getOutgoingRelationships(node)) {
				if (ignoreRelTypes.contains(rel.getType().name().toLowerCase()))
					continue;
				createRelationship(targetDb, rel, ignoreProperties);
				count++;
				if (count % 1000 == 0)
					System.out.print(".");
				if (count % 100000 == 0)
					System.out.println(" " + count);
			}
		}
		System.out.println("\n copying of " + count + " relationships took "
				+ (System.currentTimeMillis() - time) + " ms.");
	}

	private static void createRelationship(BatchInserter targetDb,
			Relationship rel, Set<String> ignoreProperties) {
		long startNodeId = rel.getStartNode().getId();
		long endNodeId = rel.getEndNode().getId();
		final RelationshipType type = rel.getType();
		try {
			targetDb.createRelationship(startNodeId, endNodeId, type,
					getProperties(rel, ignoreProperties));
		} catch (InvalidRecordException ire) {
			addLog(rel, "create Relationship: " + startNodeId + "-[:" + type
					+ "]" + "->" + endNodeId, ire.getMessage());
		}
	}

	private static Iterable<Relationship> getOutgoingRelationships(Node node) {
		try {
			return node.getRelationships(Direction.OUTGOING);
		} catch (InvalidRecordException ire) {
			addLog(node, "outgoingRelationships", ire.getMessage());
			return Collections.emptyList();
		}
	}

	private static void copyNodes(GraphDatabaseService sourceDb,
			BatchInserter targetDb, Set<String> ignoreProperties) {
		final Node refNode = sourceDb.getReferenceNode();
		long time = System.currentTimeMillis();
		int count = 0;
		for (Node node : sourceDb.getAllNodes()) {
			if (node.equals(refNode)) {
				targetDb.setNodeProperties(targetDb.getReferenceNode(),
						getProperties(node, ignoreProperties));
			} else {
				targetDb.createNode(node.getId(),
						getProperties(node, ignoreProperties));
			}
			count++;
			if (count % 1000 == 0)
				System.out.print(".");
			if (count % 100000 == 0) {
				logs.flush();
				System.out.println(" " + count);
			}
		}
		System.out.println("\n copying of " + count + " nodes took "
				+ (System.currentTimeMillis() - time) + " ms.");
	}

	private static Map<String, Object> getProperties(PropertyContainer pc,
			Set<String> ignoreProperties) {
		Map<String, Object> result = new HashMap<String, Object>();
		for (String property : getPropertyKeys(pc)) {
			if (ignoreProperties.contains(property.toLowerCase()))
				continue;
			try {
				result.put(property, pc.getProperty(property));
			} catch (InvalidRecordException ire) {
				addLog(pc, property, ire.getMessage());
			}
		}
		return result;
	}

	private static Iterable<String> getPropertyKeys(PropertyContainer pc) {
		try {
			return pc.getPropertyKeys();
		} catch (InvalidRecordException ire) {
			addLog(pc, "propertyKeys", ire.getMessage());
			return Collections.emptyList();
		}
	}

	private static void addLog(PropertyContainer pc, String property,
			String message) {
		logs.append(String.format("%s.%s %s%n", pc, property, message));
	}
}