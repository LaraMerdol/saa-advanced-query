package org.ivis.visuall;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.driver.v1.StatementResult;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

// below cypher script is used to export graph as cypher script
// CALL apoc.export.cypher.query("match (n)-[r]->(n2) return * limit 100", "subset.cypher", 
// {format:'plain',separateFiles:false, cypherFormat: 'create', useOptimizations:{type: "NONE", unwindBatchSize: 20}})
// YIELD file, batches, source, format, nodes, relationships, time, rows, batchSize
// RETURN file, batches, source, format, nodes, relationships, time, rows, batchSize;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AdvancedQueryTest {
    private static final Config driverConfig = Config.build().withoutEncryption().toConfig();
    private ServerControls embeddedDatabaseServer;
    private static String paperFig11Graph = "CREATE (EPHA3:P{n:'EPHA3'}) CREATE (EPS15:P{n:'EPS15'}) CREATE (P140:P{n:'P140'})"
            + "CREATE (MAP4K1:P{n:'MAP4K1'}) CREATE (EPHB3:P{n:'EPHB3'})"
            + "CREATE (CRKL:P{n:'CRKL'}) CREATE (MAP4K5:P{n:'MAP4K5'}) CREATE (CRK:P{n:'CRK'}) CREATE (PDGFRB:P{n:'PDGFRB'})"
            + "CREATE (PDGFRA:P{n:'PDGFRA'}) CREATE (SOS1:P{n:'SOS1'}) CREATE (GRB2:P{n:'GRB2'}) CREATE (CBLC:P{n:'CBLC'})"
            + "CREATE (GRB7:P{n:'GRB7'}) CREATE (ERBB2:P{n:'ERBB2'}) CREATE (MUC1:P{n:'MUC1'}) CREATE (RAPGEP:P{n:'RAPGEP'}) CREATE (PTK2:P{n:'PTK2'})"
            + "CREATE (RET:P{n:'RET'}) CREATE (SRC:P{n:'SRC'}) CREATE (DAB2:P{n:'DAB2'}) CREATE (SMAD2:P{n:'SMAD2'}) CREATE (NEDD9:P{n:'NEDD9'})"
            + "CREATE (EPHA3)-[:R]->(CRK), (EPS15)-[:R]->(CRK), (P140)-[:R]->(CRK), "
            + "(MAP4K1)-[:R]->(CRKL), (MAP4K1)-[:R]->(CRK), (EPHB3)-[:R]->(CRK), "
            + "(CRKL)-[:R]->(MAP4K5), (CRKL)-[:R]->(PDGFRA), (MAP4K5)-[:R]->(CRK), (MAP4K5)-[:R]->(GRB2), (CRK)-[:R]->(PDGFRA), (CRK)-[:R]->(SOS1), (CRK)-[:R]->(RAPGEP), "
            + "(CRK)-[:R]->(GRB2), (CRK)-[:R]->(PTK2), (CRK)-[:R]->(CBLC), (CRK)-[:R]->(PDGFRB), (PDGFRB)-[:R]->(GRB2),"
            + "(PDGFRA)-[:R]->(RAPGEP), (SOS1)-[:R]->(MUC1), (SOS1)-[:R]->(GRB2), (GRB2)-[:R]->(RAPGEP), (GRB2)-[:R]->(CBLC), (GRB2)-[:R]->(PTK2),"
            + "(GRB7)-[:R]->(RET), (GRB7)-[:R]->(ERBB2), (ERBB2)-[:R]->(SRC), (ERBB2)-[:R]->(MUC1), (MUC1)-[:R]->(SRC), (RAPGEP)-[:R]->(NEDD9), (PTK2)-[:R]->(NEDD9),"
            + "(RET)-[:R]->(SRC), (SRC)-[:R]->(DAB2), (DAB2)-[:R]->(SMAD2), (SMAD2)-[:R]->(NEDD9);";

    @BeforeEach
    void initializeNeo4j() {
        this.embeddedDatabaseServer = TestServerBuilders.newInProcessBuilder().withProcedure(AdvancedQuery.class)
                .newServer();
    }

    // Figure 11 in paper
    @Test
    public void GoIGiveEmpty4Length1() {

        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            session.run(AdvancedQueryTest.paperFig11Graph);

            // Then I can search for that node with lucene query syntax
            StatementResult result = session
                    .run("CALL graphOfInterest([5,7], [], 1, 2) YIELD nodes, edges return nodes, edges");

            Record r = result.single();
            Set<Long> nodeSet = r.get("nodes").asList().stream().map(x -> ((InternalNode) x).id())
                    .collect(Collectors.toSet());
            Set<Long> edgeSet = r.get("edges").asList().stream().map(x -> ((InternalRelationship) x).id())
                    .collect(Collectors.toSet());

            ArrayList<Long> trueNodeSet = new ArrayList<Long>();
            trueNodeSet.add(new Long(5));
            trueNodeSet.add(new Long(7));

            assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
            assertThat(edgeSet.size()).isEqualTo(0);
        }
    }

    @Test
    public void GoIForLength2() {

        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            session.run(AdvancedQueryTest.paperFig11Graph);

            // Then I can search for that node with lucene query syntax
            StatementResult result = session
                    .run("CALL graphOfInterest([5,7], [], 2, 2) YIELD nodes, edges return nodes, edges");

            Record r = result.single();
            Set<Long> nodeSet = r.get("nodes").asList().stream().map(x -> ((InternalNode) x).id())
                    .collect(Collectors.toSet());
            Set<Long> edgeSet = r.get("edges").asList().stream().map(x -> ((InternalRelationship) x).id())
                    .collect(Collectors.toSet());
            ArrayList<Long> trueNodeSet = new ArrayList<Long>();
            trueNodeSet.add(new Long(3));
            trueNodeSet.add(new Long(5));
            trueNodeSet.add(new Long(6));
            trueNodeSet.add(new Long(7));
            trueNodeSet.add(new Long(9));

            ArrayList<Long> trueEdgeSet = new ArrayList<Long>();
            trueEdgeSet.add(new Long(3));
            trueEdgeSet.add(new Long(4));
            trueEdgeSet.add(new Long(6));
            trueEdgeSet.add(new Long(7));
            trueEdgeSet.add(new Long(8));
            trueEdgeSet.add(new Long(10));

            assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
            assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
        }
    }

    @Test
    public void GoIForLength3() {

        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            session.run(AdvancedQueryTest.paperFig11Graph);

            // Then I can search for that node with lucene query syntax
            StatementResult result = session
                    .run("CALL graphOfInterest([5,7], [], 3, 2) YIELD nodes, edges return nodes, edges");

            Record r = result.single();
            Set<Long> nodeSet = r.get("nodes").asList().stream().map(x -> ((InternalNode) x).id())
                    .collect(Collectors.toSet());
            Set<Long> edgeSet = r.get("edges").asList().stream().map(x -> ((InternalRelationship) x).id())
                    .collect(Collectors.toSet());
            ArrayList<Long> trueNodeSet = new ArrayList<Long>();
            trueNodeSet.add(new Long(3));
            trueNodeSet.add(new Long(5));
            trueNodeSet.add(new Long(6));
            trueNodeSet.add(new Long(7));
            trueNodeSet.add(new Long(8));
            trueNodeSet.add(new Long(9));
            trueNodeSet.add(new Long(10));
            trueNodeSet.add(new Long(11));
            trueNodeSet.add(new Long(12));
            trueNodeSet.add(new Long(16));
            trueNodeSet.add(new Long(17));

            ArrayList<Long> trueEdgeSet = new ArrayList<Long>();
            trueEdgeSet.add(new Long(3));
            trueEdgeSet.add(new Long(4));
            trueEdgeSet.add(new Long(6));
            trueEdgeSet.add(new Long(7));
            trueEdgeSet.add(new Long(8));
            trueEdgeSet.add(new Long(9));
            trueEdgeSet.add(new Long(10));
            trueEdgeSet.add(new Long(11));
            trueEdgeSet.add(new Long(12));
            trueEdgeSet.add(new Long(13));
            trueEdgeSet.add(new Long(14));
            trueEdgeSet.add(new Long(15));
            trueEdgeSet.add(new Long(16));
            trueEdgeSet.add(new Long(17));
            trueEdgeSet.add(new Long(18));
            trueEdgeSet.add(new Long(20));
            trueEdgeSet.add(new Long(21));
            trueEdgeSet.add(new Long(22));
            trueEdgeSet.add(new Long(23));

            assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
            assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
        }
    }

    @Test
    public void GoIOnImdb() {
        // This is in a try-block, to make sure we close the driver after the test
        try (Driver drv = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123"));
                Session session = drv.session()) {

            // find 1 common downstream of 3 nodes
            StatementResult result = session
                    .run("CALL graphOfInterest([5,7], [], 3, 2) YIELD nodes, edges return nodes, edges");

            InternalNode n = (InternalNode) result.single().get("nodes").asList().get(0);
            assertThat(n.id()).isEqualTo(5);
        }
    }

    @Test
    public void GoIOnImdb100() {
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                Session session = driver.session()) {
            // And given I have a node in the database
            
            String s = this.readFile("C:\\dev\\visuall-advanced-query\\src\\test\\java\\org\\ivis\\visuall\\imdb100.cypher").replaceAll("\n", "");
            String[] arr = s.split(";");
            for (String cql : arr) {
                session.run(cql);
            }

            StatementResult result = session
                    .run("CALL graphOfInterest([5,7], [], 3, 2) YIELD nodes, edges return nodes, edges");

            Record r = result.single();
            Set<Long> nodeSet = r.get("nodes").asList().stream().map(x -> ((InternalNode) x).id())
                    .collect(Collectors.toSet());
            Set<Long> edgeSet = r.get("edges").asList().stream().map(x -> ((InternalRelationship) x).id())
                    .collect(Collectors.toSet());
            ArrayList<Long> trueNodeSet = new ArrayList<Long>();
            
            ArrayList<Long> trueEdgeSet = new ArrayList<Long>();
            
            assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
            assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
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

    private String readFile(String filePath) {
        StringBuilder contentBuilder = new StringBuilder();

        try (Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return contentBuilder.toString();
    }
}