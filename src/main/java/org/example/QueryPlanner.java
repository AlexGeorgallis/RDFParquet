package org.example;

import java.util.*;

public class QueryPlanner {

    // Pick a variable to sort a leaf on
    public static String determineLeafSortOn(org.example.SparqlParser.TriplePattern tp) {
        if (tp.subject.startsWith("?"))   return tp.subject;
        if (tp.predicate.startsWith("?")) return tp.predicate;
        if (tp.object.startsWith("?"))    return tp.object;
        return null;
    }

    // Variables present in a triple pattern
    public static Set<String> patternVars(org.example.SparqlParser.TriplePattern tp) {
        Set<String> vs = new HashSet<>(3);
        if (tp.subject.startsWith("?"))   vs.add(tp.subject);
        if (tp.predicate.startsWith("?")) vs.add(tp.predicate);
        if (tp.object.startsWith("?"))    vs.add(tp.object);
        return vs;
    }

    /**
     * Build a join tree over the materialized leaf rows.
     * Greedy heuristic
     */
    public static Node buildJoinTreeOverRows(List<Node> leaves) {
        if (leaves.isEmpty()) return null;
        if (leaves.size() == 1) return leaves.get(0);

        List<Node> work = new ArrayList<>(leaves);

        while (work.size() > 1) {
            long bestCost = Long.MAX_VALUE;
            int bestI = -1, bestJ = -1;
            String bestVar = null;

            // sort by size
            work.sort(Comparator.comparingLong(n -> n.estSize));

            for (int i = 0; i < work.size() - 1; i++) {
                Node A = work.get(i);
                for (int j = i + 1; j < work.size(); j++) {
                    Node B = work.get(j);
                    String sharedVar = findFirstSharedVariable(A.vars, B.vars);
                    if (sharedVar != null) {
                        long cost = Math.min(A.estSize, B.estSize);
                        if (cost < bestCost) {
                            bestCost = cost;
                            bestI = i;
                            bestJ = j;
                            bestVar = sharedVar;
                        }
                    }
                }
            }

            // No shared vars -> cross-join the two smallest
            if (bestI < 0) {
                bestI = 0;
                bestJ = 1;
                bestVar = null;
                bestCost = Math.multiplyExact(work.get(0).estSize, work.get(1).estSize);
            }

            // Build join node
            Node A = work.get(bestI);
            Node B = work.get(bestJ);

            Node join = new Node();
            join.isLeaf = false;
            join.left = A;
            join.right = B;
            join.joinVar = bestVar;
            join.vars = new HashSet<>(A.vars);
            join.vars.addAll(B.vars);
            join.estSize = bestCost; // keep as long; no unnecessary int cast

            // Remove A & B (remove higher index first), then add join
            if (bestI > bestJ) {
                work.remove(bestI);
                work.remove(bestJ);
            } else {
                work.remove(bestJ);
                work.remove(bestI);
            }
            work.add(join);
        }

        return work.get(0);
    }

    private static String findFirstSharedVariable(Set<String> a, Set<String> b) {
        for (String x : a) if (b.contains(x)) return x;
        return null;
    }

    /** Tree node for join planning/execution. */
    public static class Node {
        // structure
        public boolean isLeaf;
        public Node left, right;

        // join meta
        public String joinVar;    // shared var; null -> cartesian
        public int joinId;        // for debugging
        public long estSize;      // estimated size (rows)

        // leaf-only
        public org.example.SparqlParser.TriplePattern pattern;
        public List<int[]> rows;     // materialized matches
        public Set<String> vars;     // variables present in this node
        public Integer sortedOnSlot; // optional optimization

        @Override
        public String toString() {
            if (isLeaf) return pattern.toString();
            String l = left  != null ? (left.isLeaf  ? left.pattern.toString()  : "j" + left.joinId)  : "null";
            String r = right != null ? (right.isLeaf ? right.pattern.toString() : "j" + right.joinId) : "null";
            return String.format("(%s %s %s)", l, (joinVar != null ? "⋈{" + joinVar + "}" : "×"), r);
        }
    }
}
