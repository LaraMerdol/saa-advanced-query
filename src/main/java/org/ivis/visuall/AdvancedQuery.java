package org.ivis.visuall;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
     * finds the minimal sub-graph from given nodes
     *
     * @param ids          database ids of nodes
     * @param ignoredTypes list of strings which are ignored types
     * @return Stream<Output>
     */
    @Procedure(value = "graphOfInterest", mode = Mode.WRITE)
    @Description("finds the minimal sub-graph from given nodes")
    public Stream<Output> graphOfInterest(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes,
                                          @Name("lengthLimit") long lengthLimit, @Name("isDirected") boolean isDirected,
                                          @Name("pageSize") long pageSize, @Name("currPage") long currPage, @Name("filterTxt") String filterTxt, @Name("isIgnoreCase") boolean isIgnoreCase,
                                          @Name("orderBy") String orderBy, @Name("orderDir") long orderDir) {
        BFSOutput o1 = GoI(ids, ignoredTypes, lengthLimit, isDirected, false);
        return Stream.of(this.tableFiltering(o1, pageSize, currPage, filterTxt, isIgnoreCase, orderBy, orderDir));
    }

    /**
     * returns only the count of nodes of the minimal sub-graph
     *
     * @param ids          database ids of nodes
     * @param ignoredTypes list of strings which are ignored types
     * @return Stream<Output>
     */
    @Procedure(value = "graphOfInterestCount", mode = Mode.WRITE)
    @Description("returns only the count of nodes of the minimal sub-graph")
    public Stream<LongOup> graphOfInterestCount(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes,
                                                @Name("lengthLimit") long lengthLimit, @Name("isDirected") boolean isDirected,
                                                @Name("filterTxt") String filterTxt, @Name("isIgnoreCase") boolean isIgnoreCase) {

        BFSOutput o = GoI(ids, ignoredTypes, lengthLimit, isDirected, true);
        Output r = this.filterByTxt(o, filterTxt, isIgnoreCase);
        return Stream.of(new LongOup(r.nodes.size()));
    }

    /**
     * finds the common up/down/undirected target/regulator
     *
     * @param ids          database ids of nodes
     * @param ignoredTypes list of strings which are ignored types
     * @return Stream<Output>
     */
    @Procedure(value = "commonStream", mode = Mode.WRITE)
    @Description("From specified nodes forms founds common upstream/downstream (target/regulator)")
    public Stream<Output> commonStream(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes,
                                       @Name("lengthLimit") long lengthLimit, @Name("direction") long direction,
                                       @Name("pageSize") long pageSize, @Name("currPage") long currPage, @Name("filterTxt") String filterTxt, @Name("isIgnoreCase") boolean isIgnoreCase,
                                       @Name("orderBy") String orderBy, @Name("orderDir") long orderDir) {
        BFSOutput o1 = this.CS(ids, ignoredTypes, lengthLimit, direction, false);
        return Stream.of(this.tableFiltering(o1, pageSize, currPage, filterTxt, isIgnoreCase, orderBy, orderDir));
    }

    /**
     * finds only the nodes of the common up/down/undirected target/regulator
     *
     * @param ids          database ids of nodes
     * @param ignoredTypes list of strings which are ignored types
     * @return Stream<Output>
     */
    @Procedure(value = "commonStreamCount", mode = Mode.WRITE)
    @Description("finds only the nodes of the common up/down/undirected target/regulator")
    public Stream<LongOup> commonStreamCount(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes,
                                             @Name("lengthLimit") long lengthLimit, @Name("direction") long direction,
                                             @Name("filterTxt") String filterTxt, @Name("isIgnoreCase") boolean isIgnoreCase) {
        BFSOutput o = this.CS(ids, ignoredTypes, lengthLimit, direction, true);
        Output r = this.filterByTxt(o, filterTxt, isIgnoreCase);
        return Stream.of(new LongOup(r.nodes.size()));
    }

    private Output tableFiltering(BFSOutput o, long pageSize, long currPage, String filterTxt, boolean isIgnoreCase, String orderBy, long orderDir) {
        Output r = this.filterByTxt(o, filterTxt, isIgnoreCase);

        // sort and paginate
        OrderDirection dir = num2OrderDir(orderDir);
        if (orderBy != null && dir != OrderDirection.NONE) {
            r.nodes.sort((n1, n2) -> {
                if (!n1.hasProperty(orderBy) || !n2.hasProperty(orderBy)) {
                    return 0;
                }
                Object o1 = n1.getProperty(orderBy);
                Object o2 = n2.getProperty(orderBy);

                if (o1.getClass() == Integer.class) {
                    return ((Integer) o1).compareTo((Integer) o2);
                } else if (o1.getClass() == String.class) {
                    return ((String) o1).compareTo((String) o2);
                } else if (o1.getClass() == Double.class) {
                    return ((Double) o1).compareTo((Double) o2);
                } else if (o1.getClass() == Float.class) {
                    return ((Float) o1).compareTo((Float) o2);
                }
                return 0;
            });
            if (dir == OrderDirection.DESC) {
                Collections.reverse(r.nodes);
            }
        }
        int fromIdx = (int) ((currPage - 1) * pageSize);
        int nodeCount = r.nodes.size();
        if (fromIdx < 0 || fromIdx >= nodeCount) {
            fromIdx = 0;
        }
        int toIdx = (int) (currPage * pageSize);
        if (toIdx < 0 || toIdx > r.nodes.size()) {
            toIdx = r.nodes.size();
        }
        r.nodes = r.nodes.subList(fromIdx, toIdx);

        // get all edges
        for (long edgeId : o.edges) {
            Relationship e = this.db.getRelationshipById(edgeId);
            r.edges.add(e);
            r.edgeClass.add(e.getType().name());
            r.edgeId.add(edgeId);
            long src = e.getStartNodeId();
            long tgt = e.getEndNodeId();
            ArrayList<Long> l = new ArrayList<>();
            l.add(src);
            l.add(tgt);
            r.edgeSourceTargets.add(l);
        }

        // set meta properties for nodes
        for (Node n : r.nodes) {
            r.nodeClass.add(n.getLabels().iterator().next().name());
            r.nodeId.add(n.getId());
        }

        return r;
    }

    private Output filterByTxt(BFSOutput o, String filterTxt, boolean isIgnoreCase) {
        Output r = new Output();
        // filter by text
        if (filterTxt != null && filterTxt.length() > 0) {
            if (isIgnoreCase) {
                filterTxt = filterTxt.toLowerCase();
            }
            for (long id : o.nodes) {
                Node n = this.db.getNodeById(id);
                List<String> l = n.getAllProperties().values().stream().map(Object::toString).collect(Collectors.toList());
                boolean isPassed = false;
                for (String s : l) {
                    if (isIgnoreCase) {
                        if (s.toLowerCase().contains(filterTxt)) {
                            isPassed = true;
                            break;
                        }
                    } else {
                        if (s.contains(filterTxt)) {
                            isPassed = true;
                            break;
                        }
                    }
                }
                if (isPassed) {
                    r.nodes.add(n);
                }
            }
        } else {
            for (long id : o.nodes) {
                r.nodes.add(this.db.getNodeById(id));
            }
        }
        return r;
    }

    private BFSOutput GoI(List<Long> ids, List<String> ignoredTypes, long lengthLimit, boolean isDirected, boolean isOnlyNode) {
        HashSet<Long> idSet = new HashSet<>(ids);
        HashMap<Long, LabelData> edgeLabels = new HashMap<>();
        HashMap<Long, LabelData> nodeLabels = new HashMap<>();
        for (Long id : ids) {
            nodeLabels.put(id, new LabelData(0, 0));
        }

        BFSOutput o1 = this.GoI_BFS(nodeLabels, edgeLabels, idSet, ignoredTypes, lengthLimit, Direction.OUTGOING, isDirected);
        BFSOutput o2 = this.GoI_BFS(nodeLabels, edgeLabels, idSet, ignoredTypes, lengthLimit, Direction.INCOMING, isDirected);
        o1.edges.addAll(o2.edges);
        o1.nodes.addAll(o2.nodes);

        BFSOutput r = new BFSOutput(new HashSet<>(), new HashSet<>());
        for (long edgeId : o1.edges) {
            if (edgeLabels.get(edgeId).fwd + edgeLabels.get(edgeId).rev <= lengthLimit) {
                r.edges.add(edgeId);
            }
        }
        for (long nodeId : o1.nodes) {
            if (nodeLabels.get(nodeId).fwd + nodeLabels.get(nodeId).rev <= lengthLimit) {
                r.nodes.add(nodeId);
            }
        }
        r.nodes.addAll(ids);
        r = this.removeOrphanEdges(r);
        this.purify(idSet, r);
        r = this.removeOrphanEdges(r);

        if (isOnlyNode) {
            r.edges.clear();
            return r;
        }
        return r;
    }

    /**
     * find common stream (up/down/undirected)
     *
     * @param ids          database ids of nodes
     * @param ignoredTypes list of strings which are ignored types
     * @param lengthLimit  maximum depth
     * @param direction    should be 0 or 1
     * @param isOnlyNode   if true, returns only nodes
     * @return Outputs a list of nodes and edges (Cypher does not accept HashSet)
     */
    private BFSOutput CS(List<Long> ids, List<String> ignoredTypes, long lengthLimit, long direction, boolean isOnlyNode) {

        HashSet<Long> resultNodes = new HashSet<>();
        HashSet<Long> resultEdges = new HashSet<>();

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
                resultNodes.add(id);
            }
        }

        HashSet<Long> resultNodes2 = new HashSet<>(resultNodes);

        for (Long id : ids) {
            for (long id2 : resultNodes2) {
                String cql = "MATCH p=(n1)-[*1.." + lengthLimit + "]-(n2) where ID(n1)=" + id + " AND ID(n2)=" + id2 + " return nodes(p) as nodes, relationships(p) as edges;";
                Result res = this.db.execute(cql);
                while (res.hasNext()) {
                    Map<String, Object> r = res.next();
                    List<Relationship> edges = (List<Relationship>) r.get("edges");
                    List<Node> nodes = (List<Node>) r.get("nodes");
                    if (!isOnlyNode) {
                        resultEdges.addAll(edges.stream().map(Relationship::getId).collect(Collectors.toSet()));
                    }
                    resultNodes.addAll(nodes.stream().map(Node::getId).collect(Collectors.toSet()));
                }
            }
        }

        return new BFSOutput(resultNodes, resultEdges);
    }

    /**
     * @param node2Reached keeps reached data for each node
     * @param nodeId       id of node to make BFS
     * @param ignoredTypes list of strings which are ignored types
     * @param depthLimit   deepness of BFS
     * @param dir          direction of BFS
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
     * @param n number for enum
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
     * map number to order direction, 0: OUTGOING (downstream), 1: INCOMING (upstream), 2:
     * BOTH
     *
     * @param n number for enum
     * @return OrderDirection
     */
    private OrderDirection num2OrderDir(long n) {
        OrderDirection d = OrderDirection.NONE;
        if (n == 0) {
            d = OrderDirection.ASC;
        } else if (n == 1) {
            d = OrderDirection.DESC;
        }
        return d;
    }

    /**
     * @param ignoredTypes list of strings which are ignored types
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
        return allowedEdgeTypes.toArray(new RelationshipType[0]);

    }

    /**
     * @param ids          a list of node ids
     * @param ignoredTypes list of strings which are ignored types
     * @param lengthLimit  maximum length of a path between ids in sub graph
     * @param dir          should be INCOMING or OUTGOING
     * @return Output
     */
    private BFSOutput GoI_BFS(HashMap<Long, LabelData> nodeLabels, HashMap<Long, LabelData> edgeLabels,
                              HashSet<Long> ids, List<String> ignoredTypes, long lengthLimit, Direction dir, boolean isDirected) {
        HashSet<Long> nodeSet = new HashSet<>();
        HashSet<Long> edgeSet = new HashSet<>();
        HashSet<Long> visitedNodes = new HashSet<>();

        // prepare queue
        Queue<Long> queue = new LinkedList<>(ids);

        RelationshipType[] allowedEdgeTypesArr = getValidRelationshipTypes(ignoredTypes);

        while (!queue.isEmpty()) {
            long n1 = queue.remove();
            visitedNodes.add(n1);
            Direction d = dir;
            if (!isDirected) {
                d = Direction.BOTH;
            }
            Iterable<Relationship> edges = this.db.getNodeById(n1).getRelationships(d, allowedEdgeTypesArr);
            for (Relationship e : edges) {
                long edgeId = e.getId();
                Node n2 = e.getOtherNode(this.db.getNodeById(n1));
                long n2Id = n2.getId();
                if (this.isNodeIgnored(n2, ignoredTypes) || visitedNodes.contains(n2Id)) {
                    continue;
                }
                LabelData labelE = edgeLabels.get(edgeId);
                if (labelE == null) {
                    labelE = new LabelData(lengthLimit + 1);
                }
                LabelData labelN1 = nodeLabels.get(n1);
                if (labelN1 == null) {
                    labelN1 = new LabelData(lengthLimit + 1);
                }
                if (dir == Direction.OUTGOING) {
                    labelE.fwd = labelN1.fwd + 1;
                } else if (dir == Direction.INCOMING) {
                    labelE.rev = labelN1.rev;
                }
                edgeLabels.put(edgeId, labelE);
                nodeLabels.put(n1, labelN1);

                nodeSet.add(n2Id);
                edgeSet.add(edgeId);
                LabelData labelN2 = nodeLabels.get(n2Id);
                if (labelN2 == null) {
                    labelN2 = new LabelData(lengthLimit + 1);
                }
                nodeLabels.put(n2Id, labelN2);
                if (labelN2.getLabel(dir) > labelN1.getLabel(dir) + 1) {
                    labelN2.setLabel(labelN1.getLabel(dir) + 1, dir);
                    nodeLabels.put(n2Id, labelN2);
                    if (labelN2.getLabel(dir) < lengthLimit && !ids.contains(n2Id)) {
                        queue.add(n2Id);
                    }
                }
            }
        }
        return new BFSOutput(nodeSet, edgeSet);
    }

    /**
     * @param n            node
     * @param ignoredTypes list of strings which are ignored types
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

    /**
     * chops all the nodes that are connected to only 1 node iteratively, returns a graph where each node is
     * at least degree-2 or inside the list of srcIds
     *
     * @param srcIds   ids of nodes which are sources
     * @param subGraph current sub-graph which will be modified
     */
    private void purify(HashSet<Long> srcIds, BFSOutput subGraph) {
        HashMap<Long, HashSet<Long>> node2edge = new HashMap<>();
        HashMap<Long, HashSet<Long>> node2node = new HashMap<>();
        subGraph.nodes.addAll(srcIds);
        for (long edgeId : subGraph.edges) {
            Relationship r = this.db.getRelationshipById(edgeId);
            long id1 = r.getStartNodeId();
            long id2 = r.getEndNodeId();

            this.insert2AdjList(node2edge, id1, edgeId);
            this.insert2AdjList(node2edge, id2, edgeId);
            // do not consider self-loops
            if (id1 != id2) {
                this.insert2AdjList(node2node, id1, id2);
                this.insert2AdjList(node2node, id2, id1);
            }
        }

        HashSet<Long> degree1Nodes = this.getOrphanNodes(node2node, srcIds);

        while (degree1Nodes.size() > 0) {
            for (long nodeId : degree1Nodes) {
                subGraph.nodes.remove(nodeId);
                subGraph.edges.removeAll(node2edge.get(nodeId));

                // decrement the degree of the other node (node on the other hand of currently deleted orphan node)
                HashSet<Long> otherNodeIds = node2node.get(nodeId);
                for (long id : otherNodeIds) {
                    node2node.get(id).remove(nodeId);
                }
                node2node.remove(nodeId);
            }

            degree1Nodes = this.getOrphanNodes(node2node, srcIds);
        }
    }

    private void insert2AdjList(HashMap<Long, HashSet<Long>> map, long key, long val) {
        HashSet<Long> set = map.get(key);
        if (set == null) {
            set = new HashSet<>();
        }
        set.add(val);
        map.put(key, set);
    }

    private BFSOutput removeOrphanEdges(BFSOutput elms) {
        BFSOutput result = new BFSOutput(elms.nodes, new HashSet<>());

        for (long edgeId : elms.edges) {
            Relationship r = this.db.getRelationshipById(edgeId);
            long s = r.getStartNodeId();
            long e = r.getEndNodeId();
            if (elms.nodes.contains(s) && elms.nodes.contains(e)) {
                result.edges.add(edgeId);
            }
        }
        return result;
    }

    private HashSet<Long> getOrphanNodes(HashMap<Long, HashSet<Long>> node2node, HashSet<Long> srcIds) {
        HashSet<Long> orphanNodes = new HashSet<>();
        for (Long k : node2node.keySet()) {
            if (!srcIds.contains(k) && node2node.get(k).size() == 1) {
                orphanNodes.add(k);
            }
        }
        return orphanNodes;
    }

    public static class Output {
        public List<Node> nodes;
        public List<String> nodeClass;
        public List<Long> nodeId;

        public List<Relationship> edges;
        public List<String> edgeClass;
        public List<Long> edgeId;
        public List<List<Long>> edgeSourceTargets;

        Output() {
            this.nodes = new ArrayList<>();
            this.edges = new ArrayList<>();
            this.nodeClass = new ArrayList<>();
            this.edgeClass = new ArrayList<>();
            this.nodeId = new ArrayList<>();
            this.edgeId = new ArrayList<>();
            edgeSourceTargets = new ArrayList<>();
        }
    }

    public static class LongOup {
        public long out;

        public LongOup(long n) {
            this.out = n;
        }
    }

    public static class BFSOutput {
        public HashSet<Long> nodes;
        public HashSet<Long> edges;

        BFSOutput(HashSet<Long> nodes, HashSet<Long> edges) {
            this.nodes = nodes;
            this.edges = edges;
        }
    }

    public static class LabelData {
        public long fwd;
        public long rev;

        LabelData(long fwd, long rev) {
            this.fwd = fwd;
            this.rev = rev;
        }

        LabelData(long n) {
            this.fwd = n;
            this.rev = n;
        }

        public void setLabel(long val, Direction dir) {
            if (dir == Direction.INCOMING) {
                this.rev = val;
            } else if (dir == Direction.OUTGOING) {
                this.fwd = val;
            }
        }

        public long getLabel(Direction dir) {
            if (dir == Direction.INCOMING) {
                return this.rev;
            }
            if (dir == Direction.OUTGOING) {
                return this.fwd;
            }
            return -1;
        }
    }

    public enum OrderDirection {
        // ascending
        ASC,
        // descending
        DESC,
        // no ordering
        NONE
    }
}