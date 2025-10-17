package org.example;


import org.example.SparqlParser.ParsedQuery;

import org.example.SparqlParser.TriplePattern;

import org.example.encodedTriplet.EncodedParquetQuery;

import org.example.encodedTriplet.EncodedTriplet;


import java.io.IOException;

import java.util.*;

public class QueryExec {
    private final EncodedParquetQuery engine = new EncodedParquetQuery();

    private int joinCount = 0;

    private final List<String> tempVarList = new ArrayList<>();

    private List<String> projectVars;

    private Map<String, Integer> slotOf;

    private List<TriplePattern> orderPatterns(List<TriplePattern> patterns,
                                              org.example.dictionary.DictionaryEncoder dict) {
        // Describes each pattern with simple structural signals
        class P {
            TriplePattern tp;
            int constCount;     // 0..3
            boolean predConst;
            boolean subjConst;
            boolean objConst;
            Set<String> varsIntroduced = new HashSet<>();

            P(TriplePattern t) {
                this.tp = t;
                this.subjConst = !t.subject.startsWith("?");
                this.predConst = !t.predicate.startsWith("?");
                this.objConst  = !t.object.startsWith("?");
                this.constCount = (subjConst ? 1 : 0) + (predConst ? 1 : 0) + (objConst ? 1 : 0);
                if (t.subject.startsWith("?"))   varsIntroduced.add(t.subject);
                if (t.predicate.startsWith("?")) varsIntroduced.add(t.predicate);
                if (t.object.startsWith("?"))    varsIntroduced.add(t.object);
            }
        }

        List<P> candidates = new ArrayList<>(patterns.size());
        for (TriplePattern t : patterns) candidates.add(new P(t));

        List<TriplePattern> ordered = new ArrayList<>(patterns.size());
        Set<String> bound = new HashSet<>();

        while (!candidates.isEmpty()) {

            P best = null;

            for (P p : candidates) {

                boolean connects = !Collections.disjoint(p.varsIntroduced, bound);
                int connScore = connects ? 1 : 0;

                int constScore = p.constCount;

                int shapeScore = (p.predConst ? 4 : 0) + (p.subjConst ? 2 : 0) + (p.objConst ? 1 : 0);

                if (best == null) {
                    best = p;
                } else {
                    boolean bestConnects = !Collections.disjoint(best.varsIntroduced, bound);
                    int bestConst = best.constCount;
                    int bestShape = (best.predConst ? 4 : 0) + (best.subjConst ? 2 : 0) + (best.objConst ? 1 : 0);

                    int cmp =
                            Integer.compare(connScore, bestConnects ? 1 : 0);
                    if (cmp == 0) cmp = Integer.compare(constScore, bestConst);
                    if (cmp == 0) cmp = Integer.compare(shapeScore, bestShape);
                    if (cmp == 0) cmp = best.tp.toString().compareTo(p.tp.toString());

                    if (cmp > 0) best = p;
                }
            }

            ordered.add(best.tp);
            bound.addAll(best.varsIntroduced);
            candidates.remove(best);
        }

        return ordered;
    }


    public List<int[]> execute(ParsedQuery parsed) throws IOException {
        if (parsed.patterns.isEmpty()) {
            return Collections.emptyList();
        }

        tempVarList.clear();

        Set<String> varSet = collectVariables(parsed);

        // Handle SELECT * projection

        if (parsed.selectVars.size() == 1 && "*".equals(parsed.selectVars.get(0))) {
            parsed.selectVars = new ArrayList<>(varSet);
        }

        for (String v : parsed.selectVars) {
            if (v.startsWith("?")) varSet.add(v);
        }

        // preserve projection order

        this.projectVars = new ArrayList<>(parsed.selectVars);

        // build slots map

        tempVarList.clear();

        tempVarList.addAll(varSet);

        int V = tempVarList.size();

        this.slotOf = new HashMap<>(V);

        for (int i = 0; i < V; i++) {
            slotOf.put(tempVarList.get(i), i);
        }

        List<QueryPlanner.Node> leaves = new ArrayList<>(parsed.patterns.size());

        List<TriplePattern> orderedPatterns = orderPatterns(parsed.patterns, org.example.dictionary.DictionaryEncoder.getInstance());

        boolean earlyTermination = false;

        for (int i = 0; i < orderedPatterns.size(); i++) {
            TriplePattern tp = orderedPatterns.get(i);

            List<EncodedTriplet> hits = querySinglePattern(tp);

            if (hits.isEmpty()) {
                earlyTermination = true;

                break;
            }

            QueryPlanner.Node leaf = createLeafNode(tp, hits, V);

            leaves.add(leaf);
        }

        if (earlyTermination) {
            return Collections.emptyList();
        }

        // plan & execute

        QueryPlanner.Node root = QueryPlanner.buildJoinTreeOverRows(leaves);

        assert root != null;

        List<int[]> joined = executeNode(root, slotOf);

        // apply DISTINCT and LIMIT

        return applyDistinctAndLimit(joined, parsed);
    }

    private Set<String> collectVariables(ParsedQuery parsed) {
        Set<String> varSet = new LinkedHashSet<>(parsed.patterns.size() * 3);

        for (TriplePattern tp : parsed.patterns) {
            if (tp.subject.startsWith("?")) varSet.add(tp.subject);

            if (tp.predicate.startsWith("?")) varSet.add(tp.predicate);

            if (tp.object.startsWith("?")) varSet.add(tp.object);
        }

        return varSet;
    }

    private QueryPlanner.Node createLeafNode(TriplePattern tp, List<EncodedTriplet> hits, int V) {
        String sortVar = QueryPlanner.determineLeafSortOn(tp);

        int sortSlot = (sortVar != null) ? slotOf.get(sortVar) : -1;

        boolean sVar = tp.subject.startsWith("?");

        boolean pVar = tp.predicate.startsWith("?");

        boolean oVar = tp.object.startsWith("?");

        int sSlot = sVar ? slotOf.get(tp.subject) : -1;

        int pSlot = pVar ? slotOf.get(tp.predicate) : -1;

        int oSlot = oVar ? slotOf.get(tp.object) : -1;

        List<int[]> rows = new ArrayList<>(hits.size());

        for (EncodedTriplet et : hits) {
            int[] row = new int[V];

            Arrays.fill(row, -1);

            if (sVar) row[sSlot] = et.getSubject();

            if (pVar) row[pSlot] = et.getPredicate();

            if (oVar) row[oSlot] = et.getObject();

            rows.add(row);
        }

        QueryPlanner.Node leaf = new QueryPlanner.Node();

        leaf.isLeaf = true;

        leaf.pattern = tp;

        leaf.rows = rows;

        leaf.vars = QueryPlanner.patternVars(tp);

        leaf.estSize = rows.size();

        leaf.sortedOnSlot = sortSlot;


        return leaf;
    }

    private List<int[]> applyDistinctAndLimit(List<int[]> joined, ParsedQuery parsed) {
        if (parsed.limit == 0 && !parsed.distinct) {
            return joined;
        }

        List<int[]> out = new ArrayList<>();

        if (parsed.distinct) {
            Set<List<Integer>> seen = new HashSet<>();

            int duplicatesSkipped = 0;

            for (int[] row : joined) {
                List<Integer> key = new ArrayList<>(projectVars.size());

                for (String v : projectVars) {
                    key.add(row[slotOf.get(v)]);
                }

                if (seen.add(key)) {
                    out.add(row);

                    if (parsed.limit > 0 && out.size() >= parsed.limit) {
                        break;
                    }

                } else {
                    duplicatesSkipped++;
                }
            }
        } else if (parsed.limit > 0) {

            int limit = Math.min(parsed.limit, joined.size());
            out = new ArrayList<>(joined.subList(0, limit));

        } else {
            out = joined;
        }

        return out;
    }

    public List<String> getProjectVars() {
        return projectVars;
    }

    public Map<String, Integer> getSlotOf() {
        return slotOf;
    }

    private List<int[]> executeNode(QueryPlanner.Node node, Map<String, Integer> slotOf) {
        if (node.isLeaf) return node.rows;

        List<int[]> L = executeNode(node.left, slotOf);

        List<int[]> R = executeNode(node.right, slotOf);

        if (L.isEmpty() || R.isEmpty()) {
            System.out.println("one side is empty");

            return Collections.emptyList();
        }

        int myId = ++joinCount;

        node.joinId = myId;

        return performJoin(node, L, R, slotOf);
    }

    private List<int[]> performJoin(QueryPlanner.Node node, List<int[]> L, List<int[]> R, Map<String, Integer> slotOf) {
        List<int[]> out = new ArrayList<>();

        if (node.joinVar != null) {
            int s = slotOf.get(node.joinVar);

            Comparator<int[]> cmp = Comparator.comparingInt(a -> a[s]);

            // Check if we need to sort

            boolean leftNeedSort = node.left.sortedOnSlot != s;

            boolean rightNeedSort = node.right.sortedOnSlot != s;

            if (leftNeedSort || rightNeedSort) {
                if (leftNeedSort) {
                    L.sort(cmp);
                }

                if (rightNeedSort) {
                    R.sort(cmp);
                }
            }

            // Merge join implementation

            int i = 0, j = 0, n = L.size(), m = R.size();

            int joinMatches = 0;

            while (i < n && j < m) {
                int lv = L.get(i)[s], rv = R.get(j)[s];

                if (lv < rv) {
                    i++;
                } else if (lv > rv) {
                    j++;
                } else {
                    int i0 = i, j0 = j;

                    while (i < n && L.get(i)[s] == lv) i++;

                    while (j < m && R.get(j)[s] == rv) j++;

                    int leftMatches = i - i0;

                    int rightMatches = j - j0;

                    int expectedSize = leftMatches * rightMatches;

                    joinMatches++;

                    if (expectedSize > 100000) {

                        List<int[]> batch = new ArrayList<>(expectedSize);

                        for (int a = i0; a < i; a++) {
                            for (int b = j0; b < j; b++) {
                                int[] merged = L.get(a).clone();

                                int[] rrow = R.get(b);

                                for (int k = 0; k < merged.length; k++) {
                                    if (merged[k] < 0 && rrow[k] >= 0) {
                                        merged[k] = rrow[k];
                                    }
                                }

                                batch.add(merged);
                            }
                        }

                        out.addAll(batch);
                    } else {
                        // Normal case

                        for (int a = i0; a < i; a++) {
                            for (int b = j0; b < j; b++) {
                                int[] merged = L.get(a).clone();

                                int[] rrow = R.get(b);

                                for (int k = 0; k < merged.length; k++) {
                                    if (merged[k] < 0 && rrow[k] >= 0) {
                                        merged[k] = rrow[k];
                                    }
                                }

                                out.add(merged);
                            }
                        }
                    }
                }
            }

            node.sortedOnSlot = s;
        } else {
            // Cross join with size estimation

            long estimatedSize = (long) L.size() * R.size();

            if (estimatedSize > Integer.MAX_VALUE) {
                throw new RuntimeException("Cross join result too large: " + estimatedSize);
            }

            out = new ArrayList<>((int) estimatedSize);

            for (int[] la : L) {
                for (int[] rb : R) {
                    int[] merged = la.clone();

                    for (int k = 0; k < merged.length; k++) {
                        if (merged[k] < 0 && rb[k] >= 0) {
                            merged[k] = rb[k];
                        }
                    }

                    out.add(merged);
                }
            }

            node.sortedOnSlot = -1;
        }

        return out;
    }

    private List<EncodedTriplet> querySinglePattern(TriplePattern tp) throws IOException {
        int pattern = 0;

        if (tp.subject.startsWith("?")) pattern |= 1;

        if (tp.predicate.startsWith("?")) pattern |= 2;

        if (tp.object.startsWith("?")) pattern |= 4;

        List<EncodedTriplet> result;

        switch (pattern) {
            case 0:

                result = engine.querySPOIds(tp);

                break;

            case 1:

                result = engine.queryPOIds(tp);

                break;

            case 2:

                result = engine.querySOIds(tp);

                break;

            case 3:

                result = engine.queryObjectOnlyIds(tp);

                break;

            case 4:

                result = engine.querySPIds(tp);

                break;

            case 5:

                result = engine.queryPredicateOnlyIds(tp);

                break;

            case 6:

                result = engine.querySubjectOnlyIds(tp);

                break;

            default:

                result = engine.queryAllIds();

                break;
        }

        return result;
    }
}