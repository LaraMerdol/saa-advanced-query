package org.ivis.visuall;

import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.stream.Stream;

import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.logging.Log;
import org.neo4j.graphdb.Direction;

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
    public Stream<Output> graphOfInterest(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes,
            @Name("lengthLimit") Long lengthLimit, @Name("isDirected") Boolean isDirected) {
        ArrayList<Node> nodes = new ArrayList<>();

        Node n = this.db.getNodeById(1);
        nodes.add(n);

        ArrayList<Relationship> edges = new ArrayList<>();
        for (Relationship r : n.getRelationships()) {
            edges.add(r);
            nodes.add(r.getEndNode());
        }
        Direction d = Direction.BOTH;
        if (isDirected) {
            d = Direction.OUTGOING;
        }
        this.GoI_BFS(ids, ignoredTypes, lengthLimit, d);

        return Stream.of(new Output(nodes, edges));
    }

    // ids: a list of node ids
    private ArrayList<String> GoI_BFS(List<Long> ids, List<String> ignoredTypes, Long lengthLimit,
            Direction d) {
        // used to store label values of graph elements
        HashMap<Long, LabelData> tmpNodes = new HashMap<>();
        HashMap<Long, LabelData> tmpEdges = new HashMap<>();

        // prepare queue
        Queue<Long> queue = new LinkedList<>();
        for (Long id : ids) {
            queue.add(id);
            tmpNodes.put(id, new LabelData(0, 0));
        }

        // prepare the edge types and direction
        ArrayList<RelationshipType> allowedEdgeTypes = new ArrayList<>();
        ResourceIterable<RelationshipType> allEdgeTypes = this.db.getAllRelationshipTypes();
        for (RelationshipType r : allEdgeTypes) {
            String name = r.name();
            if (!ignoredTypes.contains(name)) {
                allowedEdgeTypes.add(r);
            }
        }
        RelationshipType[] allowedEdgeTypesArr = allowedEdgeTypes
                .toArray(new RelationshipType[allowedEdgeTypes.size()]);

        while (!queue.isEmpty()) {
            long n1 = queue.remove();
            Iterable<Relationship> edges = this.db.getNodeById(n1).getRelationships(d, allowedEdgeTypesArr);
            for (Relationship e : edges) {
                if (d == Direction.OUTGOING) {
                    String edgeId = "e" + e.getId();
                    LabelData elem = tmp.get(edgeId);
                    if (elem == null) {
                        tmp.put(edgeId, new LabelData(labelFwd, labelRev))
                    }
                } else if (d == Direction.INCOMING) {

                }
            }
        }

        ArrayList<String> r = new ArrayList<>();

        return r;

    }

    public class Output {
        public List<Node> nodes;
        public List<Relationship> edges;

        Output(List<Node> nodes, List<Relationship> edges) {
            this.nodes = nodes;
            this.edges = edges;
        }
    }

    public class LabelData {
        public int fwd;
        public int rev;

        LabelData(int fwd, int rev) {
            this.fwd = fwd;
            this.rev = rev;
        }
    }

}