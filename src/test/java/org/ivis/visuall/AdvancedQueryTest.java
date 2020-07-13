package org.ivis.visuall;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.driver.v1.StatementResult;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AdvancedQueryTest {
    private static final Config driverConfig = Config.build().withoutEncryption().toConfig();
    private ServerControls embeddedDatabaseServer;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = TestServerBuilders.newInProcessBuilder().withProcedure(AdvancedQuery.class)
                .newServer();
    }

    @Test
    public void shouldAllowIndexingAndFindingANode() {

        // This is in a try-block, to make sure we close the driver after the test
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            session.run(
                    "CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})"
                            + "CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})"
                            + "CREATE (Carrie:Person {name:'Carrie-Anne Moss', born:1967})"
                            + "CREATE (Laurence:Person {name:'Laurence Fishburne', born:1961})"
                            + "CREATE (Hugo:Person {name:'Hugo Weaving', born:1960})"
                            + "CREATE (LillyW:Person {name:'Lilly Wachowski', born:1967})"
                            + "CREATE (LanaW:Person {name:'Lana Wachowski', born:1965})"
                            + "CREATE (JoelS:Person {name:'Joel Silver', born:1952})" + "CREATE"
                            + "(Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrix),"
                            + "(Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrix),"
                            + "(Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrix),"
                            + "(Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrix),"
                            + "(LillyW)-[:DIRECTED]->(TheMatrix)," + "(LanaW)-[:DIRECTED]->(TheMatrix),"
                            + "(JoelS)-[:PRODUCED]->(TheMatrix);");

            // Then I can search for that node with lucene query syntax
            StatementResult result = session
                    .run("CALL graphOfInterest([1,2,3], [], 1, false) YIELD nodes, edges return nodes, edges");
            assertThat(result.single().get("nodeId").asLong()).isEqualTo(0);

        }
    }
}