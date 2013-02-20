package de.scandio.blog.neo4j.test.unit;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.shell.ShellSettings;

public abstract class Neo4jBaseTest {

	private static WrappingNeoServerBootstrapper NEO4J_SERVER;
	
	public GraphDatabaseAPI api;
	public ExecutionEngine engine;
	
	public Log log = LogFactory.getLog(getClass());
	
	/*
	 * Start the Neo4j before the test begins
	 * 
	 * */
	@BeforeClass
	public static void setup(){
		GraphDatabaseAPI gda = GET_GRAPH_DATABASE_API();
		NEO4J_SERVER = new WrappingNeoServerBootstrapper( gda );
		NEO4J_SERVER.start();
	}
	
	/*
	 * Extract variables from the static neo4j service instance for local usage and insert test data from child test class
	 * 
	 * */
	@Before
	public void beforeEachTest(){
		
		api = NEO4J_SERVER.getServer().getDatabase().getGraph();
		Assert.assertNotNull(api);

		engine = new ExecutionEngine(api);
		Assert.assertNotNull(engine);
		
		insertTestData();
		
	}
	
	/*
	 * Method to insert all test data form child test class and add these nodes to an index called "__types__"
	 * Adding the nodes to an index is useful for doing cypher like 
	 * start n=node:__types__(type='user') ...
	 * instead of 
	 * start n=node(*) ...
	 * This is useful in case of you have no id from where to start your graph query. 
	 * 
	 * */
	private void insertTestData(){
		doInTransaction(new TransactionCallback() {
			public void execute(GraphDatabaseAPI api) {
				Index<Node> index = api.index().forNodes("__types__");
				Set<Node> nodes = getTestData(api);
				for(Node node:nodes){
					node.setProperty("type", "user");
					index.add(node, "type", node.getProperty("type"));
				}
				
			}
		});
	}
	
	/*
	 * All child test classes have to implement this method
	 * 
	 */
	public abstract Set<Node> getTestData(GraphDatabaseAPI api);
	
	
	/*
	 * Clean up index and delete the whole data directory
	 * 
	 * */
	@After
	public void afterEachTest() throws IOException{
		
		ExecutionResult result = executeQuery("start n=node:__types__(type='user') return n");
		final Set<Node> nodes = nodeSetFromResult(result, "n");
		
		doInTransaction(new TransactionCallback() {
			
			public void execute(GraphDatabaseAPI api) {
				for(Node node:nodes)
					api.index().forNodes("__types__").remove(node, "type");
			}
		});
		
		FileUtils.deleteRecursively(new File(NEO4J_SERVER.getServer().getDatabase().getLocation()));
		
	}
	
	/*
	 * Stop server after test
	 * 
	 * */
	@AfterClass
	public static void tearDown(){
		NEO4J_SERVER.stop();
	}
	
	/*
	 * Helper method to get an instance of the graph database api
	 * 
	 * */
	private static GraphDatabaseAPI GET_GRAPH_DATABASE_API(){
		
		GraphDatabaseFactory gdbf = new GraphDatabaseFactory();

		GraphDatabaseBuilder edbb = gdbf.newEmbeddedDatabaseBuilder( "target/neo4jTestDb" );
		edbb.setConfig( ShellSettings.remote_shell_enabled, Settings.TRUE );

		return (GraphDatabaseAPI) edbb.newGraphDatabase();
        
	}
	
	/*
	 * Helper method to encapsulate a transaction. Callback pattern is used
	 * 
	 * */
	public void doInTransaction(TransactionCallback txCallback){
		Transaction tx = api.beginTx();
		try {
			txCallback.execute(api);
			tx.success();
        } finally {
        	tx.finish();
        }
	}
	
	
	/*
	 * Simple Callback for calling back during a transaction
	 * 
	 * */
	interface TransactionCallback{
		void execute(GraphDatabaseAPI api);
	}
	
	/*
	 * Helper method to encapsulate the execution of a cypher query via the Neo4j execution engine.
	 * 
	 * */
	public ExecutionResult executeQuery(String query){
		return engine.execute(query);
	}
	
	/*
	 * Helper method to convert an execution result to a collection 
	 * 
	 */
	public Set<Node> nodeSetFromResult(ExecutionResult result, String column){
		Set<Node> nodes = new HashSet<Node>();
		
		Iterator<Node> columnIterator = result.columnAs(column);
		IteratorUtil.addToCollection(columnIterator, nodes);
		
		return nodes;
		
	}

	
}
