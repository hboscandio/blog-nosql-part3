package de.scandio.blog.neo4j.test.unit;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;

public class Neo4jCrudTest extends Neo4jBaseTest {

	@Test
	public void smithersWhoIsThatGuySleeping() {

		String query = "START n=node:__types__(type='user') WHERE n.firstname = 'Homer' return n";

		ExecutionResult result = executeQuery(query);
		final Set<Node> nodes = nodeSetFromResult(result, "n");

		Iterator<Node> columnIterator = result.columnAs("n");
		IteratorUtil.addToCollection(columnIterator, nodes);

		// Okily-dokily if there is only one result - Homer itself
		Assert.assertEquals(1, nodes.size());

		// Smithers, who is this nincompoop?
		Node node = nodes.iterator().next();
		Assert.assertEquals("Homer", node.getProperty("firstname"));
		Assert.assertEquals("Simpson", node.getProperty("lastname"));
		Assert.assertEquals("safety supervisor", node.getProperty("job"));

	}

	@Test
	public void howManyChildrenHasHomers() {

		String query = "START n=node:__types__(type='user') MATCH n-[c:IS_FATHER_OF]->() return COUNT(c)";
		ExecutionResult result = executeQuery(query);
		final Set<Node> nodes = nodeSetFromResult(result, "COUNT(c)");

		// Okily-dokily if homer has 3 children.
		Assert.assertEquals(3L, nodes.iterator().next());

	}

	@Test
	public void whoAreHomersChildren() {

		String query = "START n=node:__types__(type='user') MATCH n-[:IS_CHILD_OF]->() return n";

		ExecutionResult result = executeQuery(query);
		final Set<Node> nodes = nodeSetFromResult(result, "n");

		// Okily-dokily if there are exact 3 matches - Bartholomew Simpson, Lisa
		// Marie Simpson and Maggie Simpson
		Assert.assertEquals(3, nodes.size());

	}

	@Test
	public void whoShotMrBurns() {

		String queryForBurns = "START n=node:__types__(type='user') where n.lastname = 'Burns' return n";
		String queryForMaggie = "START n=node:__types__(type='user') where n.firstname = 'Maggie' return n";
		
		ExecutionResult resultForBurns = executeQuery(queryForBurns);
		ExecutionResult resultForMaggie = executeQuery(queryForMaggie);
		
		final Set<Node> burnsInASet = nodeSetFromResult(resultForBurns, "n");
		final Set<Node> maggieInASet = nodeSetFromResult(resultForMaggie, "n");
		
		// Is there really only Burns and Maggie on stage?
		Assert.assertEquals(1, burnsInASet.size());
		Assert.assertEquals(1, maggieInASet.size());

		final Node burns = burnsInASet.iterator().next();
		final Node maggie = maggieInASet.iterator().next();
		
		doInTransaction(new TransactionCallback() {
			public void execute(GraphDatabaseAPI api) {
				
				burns.delete();
				maggie.setProperty("canSeeTheSunAgain", true);
				
			}
		});

		resultForBurns = executeQuery(queryForBurns);
		// Is Burns still alive?
		Assert.assertEquals(0, nodeSetFromResult(resultForBurns, "n").size());
		
		
	}

	@Override
	public Set<Node> getTestData(GraphDatabaseAPI api) {

		Node user1 = api.createNode();
		user1.setProperty("firstname", "Homer");
		user1.setProperty("lastname", "Simpson");
		user1.setProperty("job", "safety supervisor");

		Node user2 = api.createNode();
		user2.setProperty("firstname", "Bart");
		user2.setProperty("lastname", "Simpson");
		user2.setProperty("job", "pupil");

		Node user3 = api.createNode();
		user3.setProperty("firstname", "Lisa");
		user3.setProperty("lastname", "Simpson");
		user3.setProperty("job", "pupil");

		Node user4 = api.createNode();
		user4.setProperty("firstname", "Maggie");
		user4.setProperty("lastname", "Simpson");
		user4.setProperty("job", "pupil");

		Node user5 = api.createNode();
		user5.setProperty("firstname", "Charles Montgomery");
		user5.setProperty("lastname", "Burns");
		user5.setProperty("job", "Boss");

		// yeah, I know!! There is also Hugo Simpson from s04e14. Perhaps in a next tutorial around Halloween ;)

		user1.createRelationshipTo(user2, FamilyRelations.IS_FATHER_OF);
		user1.createRelationshipTo(user3, FamilyRelations.IS_FATHER_OF);
		user1.createRelationshipTo(user4, FamilyRelations.IS_FATHER_OF);

		user2.createRelationshipTo(user1, FamilyRelations.IS_CHILD_OF);
		user3.createRelationshipTo(user1, FamilyRelations.IS_CHILD_OF);
		user4.createRelationshipTo(user1, FamilyRelations.IS_CHILD_OF);

		Set<Node> testData = new HashSet<Node>();
		testData.add(user1);
		testData.add(user2);
		testData.add(user3);
		testData.add(user4);
		testData.add(user5);

		return testData;

	}

	enum FamilyRelations implements RelationshipType {

		IS_FATHER_OF("IS_FATHER_OF"), IS_CHILD_OF("IS_CHILD_OF");

		private String name;

		FamilyRelations(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

	enum EvilRelations implements RelationshipType {
		SHOT;
	}

}