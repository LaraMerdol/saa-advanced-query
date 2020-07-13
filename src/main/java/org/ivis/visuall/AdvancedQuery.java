package org.ivis.visuall;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import org.neo4j.logging.Log;

public class AdvancedQuery {
    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @Procedure(value = "graphOfInterest", mode = Mode.WRITE)
    @Description("From specified nodes forms a minimal graph of interest")
    public Stream<Output> graphOfInterest(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes, @Name("lengthLimit") Long lengthLimit, @Name("isDirected") Boolean isDirected) {
        ArrayList<Node> nodes = new ArrayList<>();
        Node n = this.db.getNodeById(1);
        nodes.add(n);
        
        ArrayList<Relationship> edges = new ArrayList<>();
        for (Relationship r : n.getRelationships()) {
            edges.add(r);
            nodes.add(r.getEndNode());
        }

        return Stream.of(new Output(nodes, edges));
    }

    public class Output {
        public List<Node> nodes;
        public List<Relationship> edges;

        Output(List<Node> nodes, List<Relationship> edges) {
            this.nodes = nodes;
            this.edges = edges;
        }

    }
}