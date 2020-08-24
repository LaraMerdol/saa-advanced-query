package org.ivis.visuall;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
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
     * @param ids          node ids to find the minimal connected graph
     * @param ignoredTypes node or edge types to be ignored
     * @param lengthLimit  maximum length of a path between "ids"
     * @param isDirected   is direction important?
     * @param pageSize     return at maximum this number of nodes, always returns "ids"
     * @param currPage     which page do yoy want to return
     * @param filterTxt    filter results by text
     * @param isIgnoreCase should ignore case in text filtering?
     * @param orderBy      order elements by a property
     * @param orderDir     order direction
     * @return minimal sub-graph
     */
    @Procedure(value = "graphOfInterest", mode = Mode.WRITE)
    @Description("finds the minimal sub-graph from given nodes")
    public Stream<Output> graphOfInterest(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes,
                                          @Name("lengthLimit") long lengthLimit, @Name("isDirected") boolean isDirected,
                                          @Name("pageSize") long pageSize, @Name("currPage") long currPage, @Name("filterTxt") String filterTxt, @Name("isIgnoreCase") boolean isIgnoreCase,
                                          @Name("orderBy") String orderBy, @Name("orderDir") long orderDir) {
        BFSOutput o1 = GoI(ids, ignoredTypes, lengthLimit, isDirected, false);
        o1.nodes.removeIf(ids::contains);
        Output o2 = this.tableFiltering(o1, pageSize - ids.size(), currPage, filterTxt, isIgnoreCase, orderBy, orderDir);
        this.addSourceNodes(o2, ids);
        return Stream.of(o2);
    }

    /**
     * returns only the count of nodes of the minimal sub-graph
     *
     * @param ids          node ids to find the minimal connected graph
     * @param ignoredTypes node or edge types to be ignored
     * @param lengthLimit  maximum length of a path between "ids"
     * @param isDirected   is direction important?
     * @param filterTxt    filter results by text
     * @param isIgnoreCase should ignore case in text filtering?
     * @return size of minimal sub-graph
     */
    @Procedure(value = "graphOfInterestCount", mode = Mode.WRITE)
    @Description("returns only the count of nodes of the minimal sub-graph")
    public Stream<LongOup> graphOfInterestCount(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes,
                                                @Name("lengthLimit") long lengthLimit, @Name("isDirected") boolean isDirected,
                                                @Name("filterTxt") String filterTxt, @Name("isIgnoreCase") boolean isIgnoreCase, @Name("pageSize") long pageSize) {

        BFSOutput o = GoI(ids, ignoredTypes, lengthLimit, isDirected, true);
        o.nodes.removeIf(ids::contains);
        Output r = this.filterByTxt(o, filterTxt, isIgnoreCase);
        this.addSourceNodes(r, ids);
        int n = r.nodes.size();
        int numPage = (int) Math.ceil((double) n / pageSize); // if we don't consider returning source nodes always
        int numExtra4SrcNodes = (numPage - 1) * ids.size();
        return Stream.of(new LongOup(n + numExtra4SrcNodes));
    }

    /**
     * finds common nodes and edges on the downstream or upstream or undirected stream
     *
     * @param ids          node ids
     * @param ignoredTypes node or edge types to be ignored
     * @param lengthLimit  maximum depth
     * @param direction    INCOMING means upstream (regulator), OUTGOING means downstream (target)
     * @param pageSize     return at maximum this number of nodes, always returns "ids"
     * @param currPage     which page do yoy want to return
     * @param filterTxt    filter results by text
     * @param isIgnoreCase should ignore case in text filtering?
     * @param orderBy      order elements by a property
     * @param orderDir     order direction
     * @return sub-graph which contains sources and targets/regulators with paths between
     */
    @Procedure(value = "commonStream", mode = Mode.WRITE)
    @Description("From specified nodes forms founds common upstream/downstream (target/regulator)")
    public Stream<Output> commonStream(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes,
                                       @Name("lengthLimit") long lengthLimit, @Name("direction") long direction,
                                       @Name("pageSize") long pageSize, @Name("currPage") long currPage, @Name("filterTxt") String filterTxt, @Name("isIgnoreCase") boolean isIgnoreCase,
                                       @Name("orderBy") String orderBy, @Name("orderDir") long orderDir) {
        BFSOutput o1 = this.CS(ids, ignoredTypes, lengthLimit, direction, false);
        o1.nodes.removeIf(ids::contains);
        Output o2 = this.tableFiltering(o1, pageSize - ids.size(), currPage, filterTxt, isIgnoreCase, orderBy, orderDir);
        this.addSourceNodes(o2, ids);
        return Stream.of(o2);
    }

    /**
     * finds size of common nodes and edges on the downstream or upstream or undirected stream
     *
     * @param ids          node ids
     * @param ignoredTypes node or edge types to be ignored
     * @param lengthLimit  maximum depth
     * @param direction    INCOMING means upstream (regulator), OUTGOING means downstream (target)
     * @param filterTxt    filter results by text
     * @param isIgnoreCase should ignore case in text filtering?
     * @return size of sub-graph which contains sources and targets/regulators with paths between
     */
    @Procedure(value = "commonStreamCount", mode = Mode.WRITE)
    @Description("finds only the nodes of the common up/down/undirected target/regulator")
    public Stream<LongOup> commonStreamCount(@Name("ids") List<Long> ids, @Name("ignoredTypes") List<String> ignoredTypes,
                                             @Name("lengthLimit") long lengthLimit, @Name("direction") long direction,
                                             @Name("filterTxt") String filterTxt, @Name("isIgnoreCase") boolean isIgnoreCase, @Name("pageSize") long pageSize) {
        BFSOutput o = this.CS(ids, ignoredTypes, lengthLimit, direction, true);
        o.nodes.removeIf(ids::contains);
        Output r = this.filterByTxt(o, filterTxt, isIgnoreCase);
        this.addSourceNodes(r, ids);
        int n = r.nodes.size();
        int numPage = (int) Math.ceil((double) n / pageSize); // if we don't consider returning source nodes always
        int numExtra4SrcNodes = (numPage - 1) * ids.size();
        return Stream.of(new LongOup(n + numExtra4SrcNodes));
    }

    /**
     * Since result could be big, gives results page by page
     *
     * @param o            to be paginated
     * @param pageSize     size of a page
     * @param currPage     current page to be returned
     * @param filterTxt    filtering text
     * @param isIgnoreCase should ignore case in text filtering?
     * @param orderBy      order by a property
     * @param orderDir     order direction
     * @return a page of o
     */
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
        if (fromIdx >= nodeCount) {
            fromIdx = nodeCount;
        }
        if (fromIdx < 0) {
            fromIdx = 0;
        }
        int toIdx = (int) (currPage * pageSize);
        if (toIdx < 0 || toIdx > nodeCount) {
            toIdx = nodeCount;
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

    /**
     * filter by text using simple text matching
     *
     * @param o            to be filtered
     * @param filterTxt    text for filtering
     * @param isIgnoreCase should ignore case?
     * @return filtered "o"
     */
    private Output filterByTxt(BFSOutput o, String filterTxt, boolean isIgnoreCase) {
        Output r = new Output();
        // filter by text
        if (filterTxt != null && filterTxt.length() > 0) {
            if (isIgnoreCase) {
                filterTxt = filterTxt.toLowerCase();
            }
            for (long id : o.nodes) {
                Node n = this.db.getNodeById(id);
                List<String> l = new ArrayList<>();
                for (Object p : n.getAllProperties().values()) {
                    if (p.getClass().isArray()) {
                        StringBuilder s = new StringBuilder();
                        for (Object o2 : ((Object[]) p)) {
                            s.append(o2.toString());
                        }
                        l.add(s.toString());
                    } else {
                        l.add(p.toString());
                    }
                }

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

    private void addSourceNodes(Output o, List<Long> ids) {
        // insert source nodes
        for (long id : ids) {
            Node n = this.db.getNodeById(id);
            o.nodeId.add(id);
            o.nodes.add(n);
            o.nodeClass.add(n.getLabels().iterator().next().name());
        }
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

        HashSet<Long> candidates = new HashSet<>();
        HashMap<Long, Integer> node2Reached = new HashMap<>();
        Direction d = this.num2Dir(direction);
        for (long id : ids) {
            candidates.addAll(this.CS_BFS(node2Reached, id, ignoredTypes, lengthLimit, d));
        }
        int size = ids.size();
        for (long id : candidates) {
            Integer i = node2Reached.get(id);
            if (i != null && i == size) {
                resultNodes.add(id);
            }
        }

        HashSet<Long> s1 = new HashSet<>(ids);
        HashMap<Long, LabelData> edgeLabels = new HashMap<>();
        HashMap<Long, LabelData> nodeLabels = new HashMap<>();
        for (Long id : ids) {
            nodeLabels.put(id, new LabelData(0, lengthLimit + 1));
        }
        for (Long id : resultNodes) {
            nodeLabels.put(id, new LabelData(lengthLimit + 1, 0));
        }

        BFSOutput o1;
        BFSOutput o2;
        if (d == Direction.OUTGOING) { // means common target
            o1 = GoI_BFS(nodeLabels, edgeLabels, s1, ignoredTypes, lengthLimit, Direction.OUTGOING, true);
            o2 = GoI_BFS(nodeLabels, edgeLabels, resultNodes, ignoredTypes, lengthLimit, Direction.INCOMING, true);
        } else if (d == Direction.INCOMING) { // means common regulator
            o1 = GoI_BFS(nodeLabels, edgeLabels, s1, ignoredTypes, lengthLimit, Direction.INCOMING, true);
            o2 = GoI_BFS(nodeLabels, edgeLabels, resultNodes, ignoredTypes, lengthLimit, Direction.OUTGOING, true);
        } else {
            o1 = GoI_BFS(nodeLabels, edgeLabels, s1, ignoredTypes, lengthLimit, Direction.OUTGOING, false);
            o2 = GoI_BFS(nodeLabels, edgeLabels, resultNodes, ignoredTypes, lengthLimit, Direction.INCOMING, false);
        }

        // merge result sets
        o1.edges.addAll(o2.edges);
        o1.nodes.addAll(o2.nodes);

        BFSOutput r = new BFSOutput(new HashSet<>(), new HashSet<>());
        if (!isOnlyNode) {
            for (long edgeId : o1.edges) {
                if (edgeLabels.get(edgeId).fwd + edgeLabels.get(edgeId).rev <= lengthLimit) {
                    r.edges.add(edgeId);
                }
            }
        }

        for (long nodeId : o1.nodes) {
            if (nodeLabels.get(nodeId).fwd + nodeLabels.get(nodeId).rev <= lengthLimit) {
                r.nodes.add(nodeId);
            }
        }
        r.nodes.addAll(ids);
        r.nodes.addAll(resultNodes);
        r = this.removeOrphanEdges(r);
        s1.addAll(resultNodes);
        this.purify(s1, r);
        return this.removeOrphanEdges(r);
    }

    /**
     * @param node2Reached keeps reached data for each node
     * @param nodeId       id of node to make BFS
     * @param ignoredTypes list of strings which are ignored types
     * @param depthLimit   deepness of BFS
     * @param dir          direction of BFS
     * @return HashSet<Long>
     */
    private HashSet<Long> CS_BFS(HashMap<Long, Integer> node2Reached, long nodeId, List<String> ignoredTypes,
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
     * breadth-first search for graph of interest. keeps "fwd" and "rev" data for each node and edge as "LabelData"
     *
     * @param nodeLabels   labels for nodes, should be persist to next calls
     * @param edgeLabels   labels for edges, should be persist to next calls
     * @param ids          initial nodes for search
     * @param ignoredTypes node or edge types to be ignored
     * @param lengthLimit  maximum length of a path between "ids"
     * @param dir          direction
     * @param isDirected   is directed?
     * @return graph elements visited
     */
    private BFSOutput GoI_BFS(HashMap<Long, LabelData> nodeLabels, HashMap<Long, LabelData> edgeLabels,
                              HashSet<Long> ids, List<String> ignoredTypes, long lengthLimit, Direction dir, boolean isDirected) {
        HashSet<Long> nodeSet = new HashSet<>();
        HashSet<Long> edgeSet = new HashSet<>();
        HashSet<Long> visitedEdges = new HashSet<>();

        // prepare queue
        Queue<Long> queue = new LinkedList<>(ids);

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
                long n2Id = n2.getId();
                if (this.isNodeIgnored(n2, ignoredTypes) || visitedEdges.contains(edgeId)) {
                    continue;
                }
                visitedEdges.add(edgeId);
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

    /**
     * remove edges that does not have source or target
     *
     * @param elms output from GoI_BFS
     * @return output from GoI_BFS
     */
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