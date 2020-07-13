package org.ivis.visuall;

import java.util.ArrayList;
import java.util.Queue;
import java.util.Set;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
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
        HashSet<Node> nodes = new HashSet<>();

        Node n = this.db.getNodeById(1);
        nodes.add(n);

        HashSet<Relationship> edges = new HashSet<>();
        for (Relationship r : n.getRelationships()) {
            edges.add(r);
            nodes.add(r.getEndNode());
        }
        Direction d = Direction.BOTH;
        if (isDirected) {
            d = Direction.OUTGOING;
        }
        this.GoI_BFS(new HashSet<>(ids), ignoredTypes, lengthLimit, d);

        return Stream.of(new Output(nodes, edges));
    }

    // ids: a list of node ids
    private Output GoI_BFS(HashSet<Long> ids, List<String> ignoredTypes, Long lengthLimit, Direction dir) {
        Output oup = new Output(new HashSet<Node>(), new HashSet<Relationship>());

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
            Iterable<Relationship> edges = this.db.getNodeById(n1).getRelationships(dir, allowedEdgeTypesArr);
            for (Relationship e : edges) {
                long edgeId = e.getId();
                LabelData labelE = tmpEdges.get(edgeId);
                if (labelE == null) {
                    labelE = new LabelData(0, 0);
                }
                LabelData labelN1 = tmpNodes.get(n1);
                if (labelN1 == null) {
                    labelN1 = new LabelData(0, 0);
                }
                if (dir == Direction.OUTGOING) {
                    labelE.fwd = labelN1.fwd + 1;
                } else if (dir == Direction.INCOMING) {
                    labelE.rev = labelN1.rev;
                } else if (dir == Direction.BOTH) {
                    labelE.fwd = labelN1.fwd + 1;
                    labelE.rev = labelN1.rev;
                }
                tmpEdges.put(edgeId, labelE);
                tmpNodes.put(n1, labelN1);

                Node n2 = e.getEndNode();
                oup.nodes.add(n2);
                oup.edges.add(e);
                LabelData labelN2 = tmpNodes.get(n2.getId());
                if (labelN2 == null) {
                    labelN2 = new LabelData(0, 0);
                }
                Direction[] directions = { Direction.OUTGOING };
                if (dir == Direction.INCOMING) {
                    directions[0] = Direction.INCOMING;
                } else if (dir == Direction.BOTH) {
                    directions = new Direction[] { Direction.INCOMING, Direction.OUTGOING };
                }
                for (Direction d : directions) {
                    if (labelN2.getLabel(d) > labelN1.getLabel(d) + 1) {
                        labelN2.setLabel(labelN1.getLabel(d) + 1, d);
                        Long n2Id = n2.getId();
                        tmpNodes.put(n2Id, labelN2);
                        if (labelN2.getLabel(d) < lengthLimit && !ids.contains(n2Id)) {
                            queue.add(n2Id);
                        }
                    }
                }
            }
        }
        return oup;
    }

    public class Output {
        public Set<Node> nodes;
        public Set<Relationship> edges;

        Output(Set<Node> nodes, Set<Relationship> edges) {
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

        public void setLabel(int val, Direction dir) {
            if (dir == Direction.INCOMING) {
                this.rev = val;
            } else if (dir == Direction.OUTGOING) {
                this.fwd = val;
            }
        }

        public int getLabel(Direction dir) {
            if (dir == Direction.INCOMING) {
                return this.rev;
            }
            if (dir == Direction.OUTGOING) {
                return this.fwd;
            }
            return -1;
        }
    }

}