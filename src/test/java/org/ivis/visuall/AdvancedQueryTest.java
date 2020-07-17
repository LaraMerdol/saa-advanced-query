package org.ivis.visuall;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.driver.v1.StatementResult;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Set;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AdvancedQueryTest {
    private static final Config driverConfig = Config.build().withoutEncryption().toConfig();
    private ServerControls embeddedDatabaseServer;

    @BeforeEach
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
                    .run("CALL graphOfInterest([1,2,3], [], 1, 1) YIELD nodes, edges return nodes, edges");
            assertThat(result.single().get("nodeId").asLong()).isEqualTo(0);

        }
    }

    @Test
    public void commonTargetTest1() {
        // This is in a try-block, to make sure we close the driver after the test
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            session.run(
                    "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                            + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                            + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                            + "CREATE (n14:Person {name:'n14'}) CREATE "
                            + "(n1)-[:KNOWS]->(n6),(n2)-[:KNOWS]->(n7),(n3)-[:KNOWS]->(n8),(n4)-[:KNOWS]->(n9),(n5)-[:KNOWS]->(n10),"
                            + "(n7)-[:KNOWS]->(n11),(n8)-[:KNOWS]->(n12),(n9)-[:KNOWS]->(n13),"
                            + "(n11)-[:KNOWS]->(n14),(n12)-[:KNOWS]->(n14),(n13)-[:KNOWS]->(n14);");

            // find 1 common downstream of 3 nodes
            StatementResult result = session
                    .run("CALL commonStream([1,2,3], [], 3, 0) YIELD nodes, edges return nodes, edges");

            InternalNode n = (InternalNode) result.single().get("nodes").asList().get(0);
            assertThat(n.id()).isEqualTo(13);
        }
    }

    @Test
    public void commonTargetTestOnImdb() {
        // This is in a try-block, to make sure we close the driver after the test
        try (Driver drv = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123"));
                Session session = drv.session()) {

            // find 1 common downstream of 3 nodes
            StatementResult result = session.run(
                    "CALL commonStream([1047255, 1049683, 1043696], [], 3, 2) YIELD nodes, edges return nodes, edges");

            InternalNode n = (InternalNode) result.single().get("nodes").asList().get(0);
            assertThat(n.id()).isEqualTo(5);
        }

    }

    @Test
    public void commonTargetTest2() {
        // This is in a try-block, to make sure we close the driver after the test
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            session.run(
                    "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                            + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                            + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                            + "CREATE (n14:Person {name:'n14'}) CREATE "
                            + "(n1)-[:KNOWS]->(n6),(n2)-[:KNOWS]->(n7),(n3)-[:KNOWS]->(n8),(n4)-[:KNOWS]->(n9),(n5)-[:KNOWS]->(n10),"
                            + "(n7)-[:KNOWS]->(n11),(n8)-[:KNOWS]->(n12),(n9)-[:KNOWS]->(n13),"
                            + "(n11)-[:KNOWS]->(n14),(n12)-[:KNOWS]->(n14),(n13)-[:KNOWS]->(n14);");

            // find 1 common downstream of 2 nodes
            StatementResult result = session
                    .run("CALL commonStream([1,3], [], 3, 0) YIELD nodes, edges return nodes, edges");

            InternalNode n = (InternalNode) result.single().get("nodes").asList().get(0);
            assertThat(n.id()).isEqualTo(13);
        }
    }

    @Test
    public void commonTargetTest3() {
        // This is in a try-block, to make sure we close the driver after the test
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            session.run(
                    "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                            + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                            + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                            + "CREATE (n14:Person {name:'n14'}) CREATE (n15:Person {name:'n15'}) CREATE "
                            + "(n1)-[:KNOWS]->(n6),(n2)-[:KNOWS]->(n7),(n3)-[:KNOWS]->(n8),(n4)-[:KNOWS]->(n9),(n5)-[:KNOWS]->(n10),"
                            + "(n7)-[:KNOWS]->(n11),(n8)-[:KNOWS]->(n12),(n9)-[:KNOWS]->(n13),"
                            + "(n11)-[:KNOWS]->(n14),(n12)-[:KNOWS]->(n14),(n13)-[:KNOWS]->(n14),"
                            + "(n11)-[:KNOWS]->(n15),(n12)-[:KNOWS]->(n15),(n13)-[:KNOWS]->(n15);");

            // find 2 common downstream of 3 nodes
            StatementResult result = session
                    .run("CALL commonStream([1,2,3], [], 3, 0) YIELD nodes, edges return nodes, edges");

            Set<Long> s = result.single().get("nodes").asList().stream().map(x -> ((InternalNode) x).id())
                    .collect(Collectors.toSet());

            assertThat(s.contains(new Long(13))).isEqualTo(true);
            assertThat(s.contains(new Long(14))).isEqualTo(true);
        }
    }

    @Test
    public void commonTargetUndirectedTest3() {
        // This is in a try-block, to make sure we close the driver after the test
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            session.run(
                    "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                            + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                            + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                            + "CREATE (n14:Person {name:'n14'}) CREATE (n15:Person {name:'n15'}) CREATE "
                            + "(n1)<-[:KNOWS]-(n6),(n2)<-[:KNOWS]-(n7),(n3)<-[:KNOWS]-(n8),(n4)<-[:KNOWS]-(n9),(n5)-[:KNOWS]->(n10),"
                            + "(n7)-[:KNOWS]->(n11),(n8)<-[:KNOWS]-(n12),(n9)-[:KNOWS]->(n13),"
                            + "(n11)-[:KNOWS]->(n14),(n12)<-[:KNOWS]-(n14),(n13)<-[:KNOWS]-(n14),"
                            + "(n11)<-[:KNOWS]-(n15),(n12)-[:KNOWS]->(n15),(n13)-[:KNOWS]->(n15);");

            // find 2 common downstream of 3 nodes
            StatementResult result = session
                    .run("CALL commonStream([1,2,3], [], 3, 2) YIELD nodes, edges return nodes, edges");

            Set<Long> s = result.single().get("nodes").asList().stream().map(x -> ((InternalNode) x).id())
                    .collect(Collectors.toSet());

            assertThat(s.contains(new Long(13))).isEqualTo(true);
            assertThat(s.contains(new Long(14))).isEqualTo(true);
        }
    }

    @Test
    public void commonRegulatorTest1() {
        // This is in a try-block, to make sure we close the driver after the test
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            session.run(
                    "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                            + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                            + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                            + "CREATE (n14:Person {name:'n14'}) CREATE "
                            + "(n1)<-[:KNOWS]-(n6),(n2)<-[:KNOWS]-(n7),(n3)<-[:KNOWS]-(n8),(n4)<-[:KNOWS]-(n9),(n5)<-[:KNOWS]-(n10),"
                            + "(n7)<-[:KNOWS]-(n11),(n8)<-[:KNOWS]-(n12),(n9)<-[:KNOWS]-(n13),"
                            + "(n11)<-[:KNOWS]-(n14),(n12)<-[:KNOWS]-(n14),(n13)<-[:KNOWS]-(n14);");

            // Then I can search for that node with lucene query syntax
            StatementResult result = session
                    .run("CALL commonStream([1,2,3], [], 3, 1) YIELD nodes, edges return nodes, edges");

            InternalNode n = (InternalNode) result.single().get("nodes").asList().get(0);
            assertThat(n.id()).isEqualTo(13);
        }
    }

    @Test
    public void shouldCommonTargetFail2Reach() {
        // This is in a try-block, to make sure we close the driver after the test
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            session.run(
                    "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                            + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                            + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                            + "CREATE (n14:Person {name:'n14'}) CREATE "
                            + "(n1)-[:KNOWS]->(n6),(n2)-[:KNOWS]->(n7),(n3)-[:KNOWS]->(n8),(n4)-[:KNOWS]->(n9),(n5)-[:KNOWS]->(n10),"
                            + "(n7)-[:KNOWS]->(n11),(n8)-[:KNOWS]->(n12),(n9)-[:KNOWS]->(n13),"
                            + "(n11)-[:KNOWS]->(n14),(n12)-[:KNOWS]->(n14),(n13)-[:KNOWS]->(n14);");

            // Then I can search for that node with lucene query syntax
            StatementResult result = session
                    .run("CALL commonStream([1,2,3], [], 1, 0) YIELD nodes, edges return nodes, edges");

            assertThat(result.single().get("nodes").asList().size()).isEqualTo(0);
        }
    }

    @Test
    public void shouldRegulatorTargetFail2Reach() {
        // This is in a try-block, to make sure we close the driver after the test
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            session.run(
                    "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                            + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                            + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                            + "CREATE (n14:Person {name:'n14'}) CREATE "
                            + "(n1)<-[:KNOWS]-(n6),(n2)<-[:KNOWS]-(n7),(n3)<-[:KNOWS]-(n8),(n4)<-[:KNOWS]-(n9),(n5)<-[:KNOWS]-(n10),"
                            + "(n7)<-[:KNOWS]-(n11),(n8)<-[:KNOWS]-(n12),(n9)<-[:KNOWS]-(n13),"
                            + "(n11)<-[:KNOWS]-(n14),(n12)<-[:KNOWS]-(n14),(n13)<-[:KNOWS]-(n14);");

            // Then I can search for that node with lucene query syntax
            StatementResult result = session
                    .run("CALL commonStream([1,2,3], [], 2, 1) YIELD nodes, edges return nodes, edges");

            assertThat(result.single().get("nodes").asList().size()).isEqualTo(0);
        }
    }
}