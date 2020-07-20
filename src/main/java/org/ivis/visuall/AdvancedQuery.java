package org.ivis.visuall;

import java.util.ArrayList;
import java.util.Queue;
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
import org.neo4j.graphdb.Label;
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

    /**
     * @param ids
     * @param @Name("ignoredTypes"
     * @return Stream<Output>
     */
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

        Direction d = this.num2Dir(direction);

        HashSet<Long> idSet = new HashSet<>(ids);
        BFSOutput o1 = this.GoI_BFS(idSet, ignoredTypes, lengthLimit, Direction.INCOMING, d != Direction.BOTH);
        BFSOutput o2 = this.GoI_BFS(idSet, ignoredTypes, lengthLimit, Direction.OUTGOING, d != Direction.BOTH);
        o1.edges.addAll(o2.edges);
        o1.nodes.addAll(o2.nodes);

        o1 = this.purify(idSet, o1);
        Output o = new Output(new ArrayList<Node>(), new ArrayList<Relationship>());
        for (Long nodeId : o1.nodes) {
            o.nodes.add(this.db.getNodeById(nodeId));
        }
        for (Long edgeId : o1.edges) {
            o.edges.add(this.db.getRelationshipById(edgeId));
        }
        return Stream.of(o);
    }

    /**
     * @param ids
     * @param @Name("ignoredTypes"
     * @return Stream<Output>
     */
    @Procedure(value = "commonStream", mode = Mode.WRITE)
    @Description("From specified nodes forms founds common upstream/downstream (target/regulator)")
    public Stream<Output> commonStream(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes,
            @Name("lengthLimit") Long lengthLimit, @Name("direction") long direction) {

        Output oup = new Output(new ArrayList<Node>(), new ArrayList<Relationship>());

        HashSet<Long> candidates = new HashSet<>();
        HashMap<Long, Integer> node2Reached = new HashMap<>();
        Direction d = this.num2Dir(direction);
        for (long id : ids) {
            candidates.addAll(this.BFS(node2Reached, id, ignoredTypes, lengthLimit, d));
        }
        int size = ids.size();
        for (long id : candidates) {
            Integer i = node2Reached.get(id);
            if (i != null && i == size) {
                oup.nodes.add(this.db.getNodeById(id));
            }
        }

        return Stream.of(oup);
    }

    /**
     * @param node2Reached
     * @param nodeId
     * @param ignoredTypes
     * @param depthLimit
     * @param dir
     * @return HashSet<Long>
     */
    private HashSet<Long> BFS(HashMap<Long, Integer> node2Reached, long nodeId, List<String> ignoredTypes,
            long depthLimit, Direction dir) {
        HashSet<Long> visitedNodes = new HashSet<>();
        visitedNodes.add(nodeId);

        Queue<Long> queue = new LinkedList<>();
        queue.add(nodeId);

        int currDepth = 0;
        int queueSizeBeforeMe = 0;
        boolean isPendingDepthIncrease = false;
        while (!queue.isEmpty()) {

            if (queueSizeBeforeMe == 0) {
                currDepth++;
                isPendingDepthIncrease = true;
            }
            if (currDepth == depthLimit + 1) {
                break;
            }

            Node curr = this.db.getNodeById(queue.remove());
            queueSizeBeforeMe--;

            Iterable<Relationship> edges = curr.getRelationships(dir, this.getValidRelationshipTypes(ignoredTypes));
            for (Relationship e : edges) {
                Node n = e.getOtherNode(curr);
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
                if (isPendingDepthIncrease) {
                    queueSizeBeforeMe = queue.size();
                    isPendingDepthIncrease = false;
                }

                queue.add(id);
            }
        }
        return visitedNodes;
    }

    /**
     * map number to direction, 0: OUTGOING (downstream), 1: INCOMING (upstream), 2:
     * BOTH
     * 
     * @param n
     * @return Direction
     */
    private Direction num2Dir(long n) {

        Direction d = Direction.BOTH;
        if (n == 0) {
            d = Direction.OUTGOING; // means downstream
        } else if (n == 1) {
            d = Direction.INCOMING; // means upstream
        }
        return d;
    }

    /**
     * @param ignoredTypes
     * @return RelationshipType[]
     */
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

    /**
     * @param ids
     * @param ignoredTypes
     * @param lengthLimit
     * @param dir          should be INCOMING or OUTGOING
     * @return Output
     */
    // ids: a list of node ids
    private BFSOutput GoI_BFS(HashSet<Long> ids, List<String> ignoredTypes, long lengthLimit, Direction dir,
            boolean isDirected) {

        HashSet<Long> nodeSet = new HashSet<Long>();
        HashSet<Long> edgeSet = new HashSet<Long>();
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
            Direction d = dir;
            if (!isDirected) {
                d = Direction.BOTH;
            }
            Iterable<Relationship> edges = this.db.getNodeById(n1).getRelationships(d, allowedEdgeTypesArr);
            for (Relationship e : edges) {
                long edgeId = e.getId();
                Node n2 = e.getOtherNode(this.db.getNodeById(n1));
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
                }
                tmpEdges.put(edgeId, labelE);
                tmpNodes.put(n1, labelN1);

                nodeSet.add(n2.getId());
                edgeSet.add(edgeId);
                LabelData labelN2 = tmpNodes.get(n2.getId());
                if (labelN2 == null) {
                    labelN2 = new LabelData(0, 0);
                }

                if (labelN2.getLabel(dir) > labelN1.getLabel(dir) + 1) {
                    labelN2.setLabel(labelN1.getLabel(dir) + 1, dir);
                    Long n2Id = n2.getId();
                    tmpNodes.put(n2Id, labelN2);
                    if (labelN2.getLabel(dir) < lengthLimit && !ids.contains(n2Id)) {
                        queue.add(n2Id);
                    }
                }
            }
        }
        return new BFSOutput(nodeSet, edgeSet);
    }

    /**
     * @param n
     * @param ignoredTypes
     * @return boolean
     */
    private boolean isNodeIgnored(Node n, List<String> ignoredTypes) {
        ignoredTypes.replaceAll(String::toLowerCase);
        for (Label l : n.getLabels()) {
            if (ignoredTypes.contains(l.name().toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    /** chops all the degree-1 nodes iteratively, returns a graph where each node is at least degree-2 or inside the list of srcIds
     * @param srcIds
     * @param elems
     * @param lengthLimit
     * @return Output
     */
    private BFSOutput purify(HashSet<Long> srcIds, BFSOutput elems) {

        HashMap<Long, Long> node2degree = new HashMap<>();
        HashMap<Long, Long> node2edge = new HashMap<>();
        HashMap<Long, Long> node2node = new HashMap<>();
        for (long edgeId : elems.edges) {
            Relationship r = this.db.getRelationshipById(edgeId);
            long id1 = r.getStartNodeId();
            long id2 = r.getEndNodeId();
            Long v1 = node2degree.get(id1);
            Long v2 = node2degree.get(id2);
            if (v1 != null) {
                v1 = new Long(0);
            }
            node2degree.put(id1, v1 + 1);
            if (v2 != null) {
                v2 = new Long(0);
            }
            node2degree.put(id2, v2 + 1);
            node2edge.put(id1, edgeId);
            node2edge.put(id2, edgeId);
            node2node.put(id1, id2);
            node2node.put(id2, id1);
        }

        HashSet<Long> degree1Nodes = this.getDegree1Nodes(node2degree, srcIds);

        while (degree1Nodes.size() > 0) {
            for (long nodeId : degree1Nodes) {
                node2degree.remove(nodeId);
                elems.nodes.remove(nodeId);

                long relatedEdge = node2edge.get(nodeId);
                elems.edges.remove(relatedEdge);

                Long otherNodeId = node2node.get(nodeId);
                node2degree.put(otherNodeId, node2degree.get(otherNodeId) - 1);
            }

            degree1Nodes = this.getDegree1Nodes(node2degree, srcIds);
        }

        return elems;
    }

    private HashSet<Long> getDegree1Nodes(HashMap<Long, Long> node2degree, HashSet<Long> srcIds) {
        HashSet<Long> degree1Nodes = new HashSet<>();
        for (Long k : node2degree.keySet()) {
            if (!srcIds.contains(k) && node2degree.get(k) == 1) {
                degree1Nodes.add(k);
            }
        }
        return degree1Nodes;
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

    public class BFSOutput {
        public HashSet<Long> nodes;
        public HashSet<Long> edges;

        BFSOutput(HashSet<Long> nodes, HashSet<Long> edges) {
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