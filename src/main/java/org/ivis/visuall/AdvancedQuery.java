package org.ivis.visuall;

import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Stream;

import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Result;
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
            @Name("lengthLimit") long lengthLimit, @Name("direction") long direction) {
        HashSet<Node> nodes = new HashSet<>();

        Node n = this.db.getNodeById(1);
        nodes.add(n);

        HashSet<Relationship> edges = new HashSet<>();
        for (Relationship r : n.getRelationships()) {
            edges.add(r);
            nodes.add(r.getEndNode());
        }

        // 0: OUTGOING, 1: INCOMING, 2: BOTH
        Direction d = Direction.BOTH;
        if (direction == 0) {
            d = Direction.OUTGOING;
        } else if (direction == 1) {
            d = Direction.INCOMING;
        }

        Output o = this.GoI_BFS(new HashSet<>(ids), ignoredTypes, lengthLimit, d);
        o = this.purify(ids, o, lengthLimit);
        return Stream.of(o);
    }

    @Procedure(value = "commonStream", mode = Mode.WRITE)
    @Description("From specified nodes forms founds common upstream/downstream (target/regulator)")
    public Stream<Output> commonStream(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes,
            @Name("lengthLimit") Long lengthLimit, @Name("isDownstream") Boolean isDownstream) {

        try {
            Output oup = new Output(new ArrayList<Node>(), new ArrayList<Relationship>());

            HashSet<Long> candidates = new HashSet<>();
            HashMap<Long, Integer> node2Reached = new HashMap<>();
            for (long id : ids) {
                candidates.addAll(this.BFS(node2Reached, id, ignoredTypes, lengthLimit, isDownstream));
            }
            int size = ids.size();
            for (long id : candidates) {
                if (node2Reached.get(id) == size) {
                    oup.nodes.add(this.db.getNodeById(id));
                }
            }

            return Stream.of(oup);
        } catch (Exception e) {
            return Stream.of(new Output(new ArrayList<Node>(), new ArrayList<Relationship>()));
        }

    }

    private HashSet<Long> BFS(HashMap<Long, Integer> node2Reached, long nodeId, List<String> ignoredTypes,
            long depthLimit, boolean isDownstream) {
        HashSet<Long> visitedNodes = new HashSet<>();
        visitedNodes.add(nodeId);
        Direction dir = Direction.INCOMING;
        if (isDownstream) {
            dir = Direction.OUTGOING;
        }

        Queue<Long> queue = new LinkedList<>();
        queue.add(nodeId);

        int currDepth = 0;
        int numNodesInLevel = 1;
        while (!queue.isEmpty()) {
            if (currDepth == depthLimit) {
                continue;
            }

            Node curr = this.db.getNodeById(queue.remove());
            numNodesInLevel--;
            if (numNodesInLevel == 0) {
                currDepth++;
            }
            Iterable<Relationship> edges = curr.getRelationships(dir, this.getValidRelationshipTypes(ignoredTypes));
            for (Relationship e : edges) {
                Node n = e.getEndNode();
                long id = n.getId();
                if (this.isNodeIgnored(n, ignoredTypes) || visitedNodes.contains(id)) {
                    continue;
                }

                Integer cnt = node2Reached.get(id);
                if (cnt == null) {
                    cnt = 0;
                }
                node2Reached.put(id, cnt + 1);
                visitedNodes.add(id);
                queue.add(id);
            }
        }
        return visitedNodes;
    }

    private RelationshipType[] getValidRelationshipTypes(List<String> ignoredTypes) {
        // prepare the edge types and direction
        ArrayList<RelationshipType> allowedEdgeTypes = new ArrayList<>();
        ResourceIterable<RelationshipType> allEdgeTypes = this.db.getAllRelationshipTypes();
        ignoredTypes.replaceAll(String::toLowerCase);
        for (RelationshipType r : allEdgeTypes) {
            String name = r.name().toLowerCase();
            if (!ignoredTypes.contains(name)) {
                allowedEdgeTypes.add(r);
            }
        }
        return allowedEdgeTypes.toArray(new RelationshipType[allowedEdgeTypes.size()]);

    }

    // ids: a list of node ids
    private Output GoI_BFS(HashSet<Long> ids, List<String> ignoredTypes, long lengthLimit, Direction dir) {

        HashSet<Node> nodeSet = new HashSet<Node>();
        HashSet<Relationship> edgeSet = new HashSet<Relationship>();
        // used to store label values of graph elements
        HashMap<Long, LabelData> tmpNodes = new HashMap<>();
        HashMap<Long, LabelData> tmpEdges = new HashMap<>();

        // prepare queue
        Queue<Long> queue = new LinkedList<>();
        for (Long id : ids) {
            queue.add(id);
            tmpNodes.put(id, new LabelData(0, 0));
        }

        RelationshipType[] allowedEdgeTypesArr = getValidRelationshipTypes(ignoredTypes);

        while (!queue.isEmpty()) {
            long n1 = queue.remove();
            Iterable<Relationship> edges = this.db.getNodeById(n1).getRelationships(dir, allowedEdgeTypesArr);
            for (Relationship e : edges) {
                long edgeId = e.getId();
                Node n2 = e.getEndNode();
                if (this.isNodeIgnored(n2, ignoredTypes)) {
                    continue;
                }
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

                nodeSet.add(n2);
                edgeSet.add(e);
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
        return new Output(new ArrayList<Node>(nodeSet), new ArrayList<Relationship>(edgeSet));
    }

    private boolean isNodeIgnored(Node n, List<String> ignoredTypes) {
        ignoredTypes.replaceAll(String::toLowerCase);
        for (Label l : n.getLabels()) {
            if (ignoredTypes.contains(l.name().toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    private Output purify(List<Long> ids, Output elems, long lengthLimit) {
        String cql = "MATCH p=(a)-[*0.." + lengthLimit
                + "]-(b) WHERE ID(a) IN {ids} AND b IN {ids} RETURN RELATIONSHIPS(p) as rels, NODES(p) as nodes";

        HashMap<String, Object> cqlParams = new HashMap<String, Object>();
        cqlParams.put("ids", ids);

        Result result = this.db.execute(cql, cqlParams);

        Output r = new Output(new ArrayList<Node>(), new ArrayList<Relationship>());

        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            List<Node> nodes = (List<Node>) row.get("nodes");
            if (nodes != null) {
                r.nodes.addAll(nodes);
            }
            List<Relationship> edges = (List<Relationship>) row.get("rels");

            if (edges != null) {
                r.edges.addAll(edges);
            }
        }
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

    public class BFSData {
        public HashMap<Long, Integer> node2Reached;
        public HashSet<Long> visitedNodes;

        BFSData(HashMap<Long, Integer> node2Reached, HashSet<Long> visitedNodes) {
            this.node2Reached = node2Reached;
            this.visitedNodes = visitedNodes;
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