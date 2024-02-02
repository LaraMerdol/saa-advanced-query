package org.ivis.visuall;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.driver.Result;

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
        private static final Config driverConfig = Config.builder().withoutEncryption().build();
        private Neo4j embeddedDatabaseServer;
        private static final String paperFig11Graph = "CREATE (EPHA3:P{n:'EPHA3'}) CREATE (EPS15:P{n:'EPS15'}) CREATE (P140:P{n:'P140'})"
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

        private static final String paperFig12Graph = """
                        CREATE (f:File {name: 'File1'})
                        CREATE (d1:Developer {name: 'Developer1'})
                        CREATE (d2:Developer {name: 'Developer2'})
                        CREATE         (c1:Commit {name: 'Commit1'})
                        CREATE (c2:Commit {name: 'Commit2'})
                        CREATE (c3:Commit {name: 'Commit3'})
                        CREATE (f1:File {name: 'File2'})

                        CREATE (d1)-[:WORKS_ON]->(f)
                        CREATE (d1)-[:REVIEW]->(c1)
                        CREATE (d2)-[:REVIEW]->(c1)
                        CREATE (d2)-[:WORKS_ON]->(f1)
                        CREATE (d2)-[:REVIEW]->(c2)
                        CREATE (d2)-[:REVIEW]->(c3)
                        CREATE (f)-[:MODIFIES]->(c1)
                        CREATE (f)-[:MODIFIES]->(c2)
                        CREATE (f)-[:MODIFIES]->(c3)
                        CREATE (f)-[:CONTAINS]->(f1);
                        """;

        @BeforeEach
        void initializeNeo4j() {
                this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder().withProcedure(AdvancedQuery.class)
                                .build();
        }

        // Figure 11 in paper
        @Test
        public void GoIGiveEmpty4Length1() {

                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {
                        session.run(AdvancedQueryTest.paperFig11Graph);
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"5\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"7\" return elementId(n)")
                                        .single().get(0).asString();
                        String query = "CALL graphOfInterest([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"], [], 1, false, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges RETURN nodes, edges";
                        Result result = session.run(query);
                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());

                        ArrayList<String> trueNodeSet = new ArrayList<>();
                        trueNodeSet.add("5");
                        trueNodeSet.add("7");
                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.size()).isEqualTo(0);
                }
        }

        @Test
        public void FMPBT() {
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {
                        session.run(AdvancedQueryTest.paperFig12Graph);
                        String file1 = session.run(
                                        "match (n) where n.name='File1' return elementId(n)")
                                        .single().get(0).asString();
                        String file2 = session.run(
                                        "match (n) where n.name='File2' return elementId(n)")
                                        .single().get(0).asString();             
                        String dev1 = session.run(
                                        "match (n) where n.name='Developer1' return elementId(n)")
                                        .single().get(0).asString();  
                        String dev2 = session.run(
                                        "match (n) where n.name='Developer2' return elementId(n)")
                                        .single().get(0).asString();                                                                                                            
                        String query = "CALL findNodesWithMostPathBetweenTable(["+"'" +file1+"' , "+"'" +file2+"'" +"], [],["+"'" +dev1+"' , "+"'" +dev2+"'" +"],'',3,3, false,\n" + //
                                        "      225, 1, null, false, 'score', 0, {}, 0, 0, 0, 10000, null)";
                        Result result = session.run(query);
                        Record r = result.single();                                         
                }
        }
        @Test
        public void GoIForLength2() {

                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        session.run(AdvancedQueryTest.paperFig11Graph);
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"5\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"7\" return elementId(n)")
                                        .single().get(0).asString();

                        String query = "CALL graphOfInterest([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"], [], 2, false, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges RETURN nodes, edges";
                        Result result = session.run(query);

                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();
                        trueNodeSet.add("3");
                        trueNodeSet.add("5");
                        trueNodeSet.add("6");
                        trueNodeSet.add("7");
                        trueNodeSet.add("9");

                        ArrayList<String> trueEdgeSet = new ArrayList<>();
                        trueEdgeSet.add("3");
                        trueEdgeSet.add("4");
                        trueEdgeSet.add("6");
                        trueEdgeSet.add("7");
                        trueEdgeSet.add("8");
                        trueEdgeSet.add("10");

                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
                }
        }

        // timeout tests SOMEHOW do NOT pass when I run all the tests but do PASS when I
        // run them individually
        @Test
        public void GoIForLength2Timeout() {

                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        session.run(AdvancedQueryTest.paperFig11Graph);
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"5\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"7\" return elementId(n)")
                                        .single().get(0).asString();

                        String query = "CALL graphOfInterest([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"], [], 2, false, 100, 1, null, false, null, 2, {}, 0, 0, 0, 1, null) YIELD nodes, edges RETURN nodes, edges";
                        Result result = session.run(query);
                        result.single();
                } catch (Exception e) {
                        assertThat(e.getMessage().contains("Timeout occurred! It takes longer than")).isEqualTo(true);
                }
        }

        @Test
        public void GoIForLength3() {

                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        session.run(AdvancedQueryTest.paperFig11Graph);
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"5\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"7\" return elementId(n)")
                                        .single().get(0).asString();

                        String query = "CALL graphOfInterest([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"], [], 3, false, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges RETURN nodes, edges";
                        Result result = session.run(query);

                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();
                        trueNodeSet.add("3");
                        trueNodeSet.add("5");
                        trueNodeSet.add("6");
                        trueNodeSet.add("7");
                        trueNodeSet.add("8");
                        trueNodeSet.add("9");
                        trueNodeSet.add("10");
                        trueNodeSet.add("11");
                        trueNodeSet.add("12");
                        trueNodeSet.add("16");
                        trueNodeSet.add("17");

                        ArrayList<String> trueEdgeSet = new ArrayList<>();
                        trueEdgeSet.add("3");
                        trueEdgeSet.add("4");
                        trueEdgeSet.add("6");
                        trueEdgeSet.add("7");
                        trueEdgeSet.add("8");
                        trueEdgeSet.add("9");
                        trueEdgeSet.add("10");
                        trueEdgeSet.add("11");
                        trueEdgeSet.add("12");
                        trueEdgeSet.add("13");
                        trueEdgeSet.add("14");
                        trueEdgeSet.add("15");
                        trueEdgeSet.add("16");
                        trueEdgeSet.add("17");
                        trueEdgeSet.add("18");
                        trueEdgeSet.add("20");
                        trueEdgeSet.add("21");
                        trueEdgeSet.add("22");
                        trueEdgeSet.add("23");

                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
                }
        }

        // timeout tests SOMEHOW do NOT pass when I run all the tests but do PASS when I
        // run them individually
        @Test
        public void GoIForLength3Timeout() {

                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        session.run(AdvancedQueryTest.paperFig11Graph);
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"5\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"7\" return elementId(n)")
                                        .single().get(0).asString();

                        String query = "CALL graphOfInterest([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"], [], 3, false, 100, 1, null, false, null, 2, {}, 0, 0, 0, 1, null) YIELD nodes, edges RETURN nodes, edges";
                        Result result = session.run(query);
                        result.single();
                } catch (Exception e) {
                        assertThat(e.getMessage().contains("Timeout occurred! It takes longer than")).isEqualTo(true);
                }
        }

        @Test
        public void GoIOnRunningInstance() {
                // This is in a try-block, to make sure we close the driver after the test
                try (Driver drv = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "12345678"));
                                Session session = drv.session()) {
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"5\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"7\" return elementId(n)")
                                        .single().get(0).asString();
                        String query = "CALL graphOfInterest([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"], [], 3, false, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges RETURN nodes, edges";
                        // find 1 common downstream of 3 nodes
                        Result result = session
                                        .run(query);

                        InternalNode n = (InternalNode) result.single().get("nodes").asList().get(0);
                        assertThat(n.elementId().substring(n.elementId().lastIndexOf(":") + 1)).isEqualTo("5");
                }
        }

        // test subset of imdb "match (n)-[r]->(n2) return * limit 100"
        @Test
        public void GoIOnImdb() {
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        String s = this.readFile("src/test/java/org/ivis/visuall/imdb100.cypher").replaceAll("\n",
                                        "");
                        String[] arr = s.split(";");
                        for (String cql : arr) {
                                session.run(cql);
                        }

                        String elementId1 = session.run(
                                        "match (n:Title) where n.primary_title = 'The Corbett-Fitzsimmons Fight' return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n:Person) where n.primary_name = 'William K.L. Dickson' return elementId(n)")
                                        .single().get(0).asString();
                        String query = "CALL graphOfInterest([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"], [], 3, false, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges RETURN nodes, edges";
                        Result result = session.run(query);

                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();

                        ArrayList<String> trueEdgeSet = new ArrayList<>();

                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
                }
        }

        @Test
        public void GoIOnImdb2() {
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {
                        String s = this.readFile("src/test/java/org/ivis/visuall/imdb100.cypher").replaceAll("\n",
                                        "");
                        String[] arr = s.split(";");
                        for (String cql : arr) {
                                session.run(cql);
                        }

                        String elementId1 = session.run(
                                        "match (n:Title) where n.primary_title = 'The Corbett-Fitzsimmons Fight' return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n:Title) where n.primary_title = \"Rip's Toast\" return elementId(n)")
                                        .single().get(0).asString();
                        String query = "CALL graphOfInterest([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"], [], 4, false, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges RETURN nodes, edges";
                        Result result = session.run(query);

                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();

                        ArrayList<String> trueEdgeSet = new ArrayList<>();

                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
                }
        }

        @Test
        public void GoIOnSOF() {
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {
                        String s = this.readFile("src/test/java/org/ivis/visuall/sofDB.cypher").replaceAll("\n",
                                        "");
                        String[] arr = s.split(";");
                        for (String cql : arr) {
                                session.run(cql);
                        }

                        String elementId1 = session.run(
                                        "match (n:Post) where n.title = 'Percentage width child element in absolutely positioned parent on Internet Explorer 7' return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session
                                        .run("match (n:Post) where n.title = 'Convert Decimal to Double?' return elementId(n)")
                                        .single().get(0).asString();
                        String query = "CALL graphOfInterest([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"], [], 3, true, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges RETURN nodes, edges";
                        Result result = session.run(query);
                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();

                        ArrayList<String> trueEdgeSet = new ArrayList<>();

                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
                }
        }

        @Test
        public void commonTargetTest1() {
                // This is in a try-block, to make sure we close the driver after the test
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {
                        session.run(
                                        "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                                                        + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                                                        + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                                                        + "CREATE (n14:Person {name:'n14'}) CREATE "
                                                        + "(n1)-[:KNOWS]->(n6),(n2)-[:KNOWS]->(n7),(n3)-[:KNOWS]->(n8),(n4)-[:KNOWS]->(n9),(n5)-[:KNOWS]->(n10),"
                                                        + "(n7)-[:KNOWS]->(n11),(n8)-[:KNOWS]->(n12),(n9)-[:KNOWS]->(n13),"
                                                        + "(n11)-[:KNOWS]->(n14),(n12)-[:KNOWS]->(n14),(n13)-[:KNOWS]->(n14);");
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"1\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"2\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId3 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"3\" return elementId(n)")
                                        .single().get(0).asString();
                        // find 1 common downstream of 3 nodes
                        Result result = session
                                        .run("CALL commonStream([\"" + elementId1 + "\", \"" + elementId2
                                                        + "\", \"" + elementId3
                                                        + "\"], [], 3, 0, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges return nodes, edges");

                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();
                        trueNodeSet.add("1");
                        trueNodeSet.add("2");
                        trueNodeSet.add("3");
                        trueNodeSet.add("6");
                        trueNodeSet.add("7");
                        trueNodeSet.add("8");
                        trueNodeSet.add("10");
                        trueNodeSet.add("11");
                        trueNodeSet.add("12");
                        trueNodeSet.add("13");
                        ArrayList<String> trueEdgeSet = new ArrayList<>();
                        trueEdgeSet.add("1");
                        trueEdgeSet.add("2");
                        trueEdgeSet.add("3");
                        trueEdgeSet.add("5");
                        trueEdgeSet.add("6");
                        trueEdgeSet.add("7");
                        trueEdgeSet.add("8");
                        trueEdgeSet.add("9");
                        trueEdgeSet.add("10");

                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
                }
        }

        @Test
        public void commonTargetTestOnRunningInstance() {
                // This is in a try-block, to make sure we close the driver after the test
                try (Driver drv = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "12345678"));
                                Session session = drv.session()) {
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"1047255\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"1049683\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId3 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"1043696\" return elementId(n)")
                                        .single().get(0).asString();
                        // find 1 common downstream of 3 nodes
                        Result result = session
                                        .run("CALL commonStream([\"" + elementId1 + "\", \"" + elementId2
                                                        + "\", \"" + elementId3
                                                        + "\"], [], 3, 2, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges return nodes, edges");

                        InternalNode n = (InternalNode) result.single().get("nodes").asList().get(0);
                        assertThat(n.elementId().substring(n.elementId().lastIndexOf(":") + 1)).isEqualTo("40960");
                }
        }

        @Test
        public void commonTargetTest2() {
                // This is in a try-block, to make sure we close the driver after the test
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {
                        session.run(
                                        "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                                                        + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                                                        + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                                                        + "CREATE (n14:Person {name:'n14'}) CREATE "
                                                        + "(n1)-[:KNOWS]->(n6),(n2)-[:KNOWS]->(n7),(n3)-[:KNOWS]->(n8),(n4)-[:KNOWS]->(n9),(n5)-[:KNOWS]->(n10),"
                                                        + "(n7)-[:KNOWS]->(n11),(n8)-[:KNOWS]->(n12),(n9)-[:KNOWS]->(n13),"
                                                        + "(n11)-[:KNOWS]->(n14),(n12)-[:KNOWS]->(n14),(n13)-[:KNOWS]->(n14);");

                        // find 1 common downstream of 2 nodes
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"1\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"3\" return elementId(n)")
                                        .single().get(0).asString();

                        String query = "CALL commonStream([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"], [], 3, 0, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges RETURN nodes, edges";
                        Result result = session.run(query);

                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();
                        trueNodeSet.add("1");
                        trueNodeSet.add("3");
                        trueNodeSet.add("6");
                        trueNodeSet.add("8");
                        trueNodeSet.add("10");
                        trueNodeSet.add("12");
                        trueNodeSet.add("13");
                        ArrayList<String> trueEdgeSet = new ArrayList<>();
                        trueEdgeSet.add("1");
                        trueEdgeSet.add("3");
                        trueEdgeSet.add("5");
                        trueEdgeSet.add("7");
                        trueEdgeSet.add("8");
                        trueEdgeSet.add("10");

                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
                }
        }

        // contains 2 target nodes
        @Test
        public void commonTargetTest3() {
                // This is in a try-block, to make sure we close the driver after the test
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

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
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"1\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"2\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId3 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"3\" return elementId(n)")
                                        .single().get(0).asString();
                        // find 1 common downstream of 3 nodes
                        Result result = session
                                        .run("CALL commonStream([\"" + elementId1 + "\", \"" + elementId2
                                                        + "\", \"" + elementId3
                                                        + "\"], [], 3, 0, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges return nodes, edges");

                        Set<String> s = result.single().get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());

                        assertThat(s.contains("13")).isEqualTo(true);
                        assertThat(s.contains("14")).isEqualTo(true);
                }
        }

        @Test
        public void commonTargetUndirectedTest3() {
                // This is in a try-block, to make sure we close the driver after the test
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

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
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"1\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"2\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId3 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"3\" return elementId(n)")
                                        .single().get(0).asString();
                        // find 1 common downstream of 3 nodes
                        Result result = session
                                        .run("CALL commonStream([\"" + elementId1 + "\", \"" + elementId2
                                                        + "\", \"" + elementId3
                                                        + "\"], [], 3, 2, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges return nodes, edges");

                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();
                        trueNodeSet.add("1");
                        trueNodeSet.add("2");
                        trueNodeSet.add("3");
                        trueNodeSet.add("6");
                        trueNodeSet.add("7");
                        trueNodeSet.add("8");
                        trueNodeSet.add("10");
                        trueNodeSet.add("11");
                        trueNodeSet.add("12");
                        trueNodeSet.add("13");
                        ArrayList<String> trueEdgeSet = new ArrayList<>();
                        trueEdgeSet.add("1");
                        trueEdgeSet.add("2");
                        trueEdgeSet.add("3");
                        trueEdgeSet.add("5");
                        trueEdgeSet.add("6");
                        trueEdgeSet.add("7");
                        trueEdgeSet.add("8");
                        trueEdgeSet.add("9");
                        trueEdgeSet.add("10");

                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
                }
        }

        @Test
        public void commonRegulatorTest1() {
                // This is in a try-block, to make sure we close the driver after the test
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        session.run(
                                        "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                                                        + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                                                        + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                                                        + "CREATE (n14:Person {name:'n14'}) CREATE "
                                                        + "(n1)<-[:KNOWS]-(n6),(n2)<-[:KNOWS]-(n7),(n3)<-[:KNOWS]-(n8),(n4)<-[:KNOWS]-(n9),(n5)<-[:KNOWS]-(n10),"
                                                        + "(n7)<-[:KNOWS]-(n11),(n8)<-[:KNOWS]-(n12),(n9)<-[:KNOWS]-(n13),"
                                                        + "(n11)<-[:KNOWS]-(n14),(n12)<-[:KNOWS]-(n14),(n13)<-[:KNOWS]-(n14);");
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"1\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"2\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId3 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"3\" return elementId(n)")
                                        .single().get(0).asString();
                        // find 1 common downstream of 3 nodes
                        Result result = session
                                        .run("CALL commonStream([\"" + elementId1 + "\", \"" + elementId2
                                                        + "\", \"" + elementId3
                                                        + "\"], [], 3, 1, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges return nodes, edges");
                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();
                        trueNodeSet.add("1");
                        trueNodeSet.add("2");
                        trueNodeSet.add("3");
                        trueNodeSet.add("6");
                        trueNodeSet.add("7");
                        trueNodeSet.add("8");
                        trueNodeSet.add("10");
                        trueNodeSet.add("11");
                        trueNodeSet.add("12");
                        trueNodeSet.add("13");
                        ArrayList<String> trueEdgeSet = new ArrayList<>();
                        trueEdgeSet.add("1");
                        trueEdgeSet.add("2");
                        trueEdgeSet.add("3");
                        trueEdgeSet.add("5");
                        trueEdgeSet.add("6");
                        trueEdgeSet.add("7");
                        trueEdgeSet.add("8");
                        trueEdgeSet.add("9");
                        trueEdgeSet.add("10");

                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
                }
        }

        @Test
        public void shouldCommonTargetFail2Reach() {
                // This is in a try-block, to make sure we close the driver after the test
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        session.run(
                                        "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                                                        + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                                                        + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                                                        + "CREATE (n14:Person {name:'n14'}) CREATE "
                                                        + "(n1)-[:KNOWS]->(n6),(n2)-[:KNOWS]->(n7),(n3)-[:KNOWS]->(n8),(n4)-[:KNOWS]->(n9),(n5)-[:KNOWS]->(n10),"
                                                        + "(n7)-[:KNOWS]->(n11),(n8)-[:KNOWS]->(n12),(n9)-[:KNOWS]->(n13),"
                                                        + "(n11)-[:KNOWS]->(n14),(n12)-[:KNOWS]->(n14),(n13)-[:KNOWS]->(n14);");
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"1\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"2\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId3 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"3\" return elementId(n)")
                                        .single().get(0).asString();
                        // find 1 common downstream of 3 nodes
                        Result result = session
                                        .run("CALL commonStream([\"" + elementId1 + "\", \"" + elementId2
                                                        + "\", \"" + elementId3
                                                        + "\"], [], 1, 0, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges return nodes, edges");

                        assertThat(result.single().get("nodes").asList().size()).isEqualTo(3);
                }
        }

        @Test
        public void shouldRegulatorTargetFail2Reach() {
                // This is in a try-block, to make sure we close the driver after the test
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        session.run(
                                        "CREATE (n1:Person {name:'n1'}) CREATE (n2:Person {name:'n2'}) CREATE (n3:Person {name:'n3'}) CREATE (n4:Person {name:'n4'}) CREATE (n5:Person {name:'n5'})"
                                                        + "CREATE (n6:Person {name:'n6'}) CREATE (n7:Person {name:'n7'}) CREATE (n8:Person {name:'n8'}) CREATE (n9:Person {name:'n9'}) CREATE (n10:Person {name:'n10'})"
                                                        + "CREATE (n11:Person {name:'n11'}) CREATE (n12:Person {name:'n12'}) CREATE (n13:Person {name:'n13'})"
                                                        + "CREATE (n14:Person {name:'n14'}) CREATE "
                                                        + "(n1)<-[:KNOWS]-(n6),(n2)<-[:KNOWS]-(n7),(n3)<-[:KNOWS]-(n8),(n4)<-[:KNOWS]-(n9),(n5)<-[:KNOWS]-(n10),"
                                                        + "(n7)<-[:KNOWS]-(n11),(n8)<-[:KNOWS]-(n12),(n9)<-[:KNOWS]-(n13),"
                                                        + "(n11)<-[:KNOWS]-(n14),(n12)<-[:KNOWS]-(n14),(n13)<-[:KNOWS]-(n14);");
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"1\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"2\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId3 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"3\" return elementId(n)")
                                        .single().get(0).asString();
                        // find 1 common downstream of 3 nodes
                        Result result = session
                                        .run("CALL commonStream([\"" + elementId1 + "\", \"" + elementId2
                                                        + "\", \"" + elementId3
                                                        + "\"], [], 2, 1, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges return nodes, edges");
                        assertThat(result.single().get("nodes").asList().size()).isEqualTo(3);
                }
        }

        @Test
        public void txtFilterTest() {
                // This is in a try-block, to make sure we close the driver after the test
                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        session.run("CREATE (n1:Person {name:'n1', primary_profession: ['actress', 'soundtrack'], age: 35}) CREATE (n2:Person {age: 29, name:'n2', primary_profession: ['actor', 'soundtrack']});");
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"1\" return elementId(n)")
                                        .single().get(0).asString();
                        Result result = session
                                        .run("CALL  graphOfInterest([\"" + elementId1
                                                        + "\"], [], 1, false, 100, 1, 'actress', false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges return nodes, edges");
                        result.single();
                }
        }

        @Test
        public void neighborhoodTest0() {

                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        session.run(AdvancedQueryTest.paperFig11Graph);
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"3\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"5\" return elementId(n)")
                                        .single().get(0).asString();

                        String query = "CALL neighborhood([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"],[],1, false, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges return nodes, edges";
                        Result result = session.run(query);

                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();
                        trueNodeSet.add("3");
                        trueNodeSet.add("5");
                        trueNodeSet.add("6");
                        trueNodeSet.add("7");
                        trueNodeSet.add("9");

                        ArrayList<String> trueEdgeSet = new ArrayList<>();
                        trueEdgeSet.add("3");
                        trueEdgeSet.add("4");
                        trueEdgeSet.add("6");
                        trueEdgeSet.add("7");

                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
                }
        }

        @Test
        public void neighborhoodTest1() {

                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        session.run(AdvancedQueryTest.paperFig11Graph);
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"3\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"5\" return elementId(n)")
                                        .single().get(0).asString();

                        String query = "CALL neighborhood([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"],[],0, false, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges return nodes, edges";
                        Result result = session.run(query);
                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();
                        trueNodeSet.add("3");
                        trueNodeSet.add("5");

                        ArrayList<String> trueEdgeSet = new ArrayList<>();
                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
                }
        }

        @Test
        public void neighborhoodTest2() {

                try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
                                Session session = driver.session()) {

                        session.run(AdvancedQueryTest.paperFig11Graph);
                        String elementId1 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"3\" return elementId(n)")
                                        .single().get(0).asString();
                        String elementId2 = session.run(
                                        "match (n) where split(elementId(n), \":\")[2] = \"5\" return elementId(n)")
                                        .single().get(0).asString();

                        String query = "CALL neighborhood([\"" + elementId1 + "\", \"" + elementId2
                                        + "\"],[],-1, false, 100, 1, null, false, null, 2, {}, 0, 0, 0, 10000, null) YIELD nodes, edges return nodes, edges";
                        Result result = session.run(query);
                        Record r = result.single();
                        Set<String> nodeSet = r.get("nodes").asList().stream()
                                        .map(x -> ((InternalNode) x).elementId()
                                                        .substring(((InternalNode) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        Set<String> edgeSet = r.get("edges").asList().stream()
                                        .map(x -> ((InternalRelationship) x).elementId().substring(
                                                        ((InternalRelationship) x).elementId().lastIndexOf(":") + 1))
                                        .collect(Collectors.toSet());
                        ArrayList<String> trueNodeSet = new ArrayList<>();
                        trueNodeSet.add("3");
                        trueNodeSet.add("5");

                        ArrayList<String> trueEdgeSet = new ArrayList<>();
                        assertThat(nodeSet.containsAll(trueNodeSet)).isEqualTo(true);
                        assertThat(edgeSet.containsAll(trueEdgeSet)).isEqualTo(true);
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