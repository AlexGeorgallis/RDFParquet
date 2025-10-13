package org.example;

import org.example.SparqlParser.TriplePattern;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class QueryPlannerTest {

    @Test
    void determineLeafSortOn_picksFirstVarInSPOOrder() {
        assertEquals("?s", QueryPlanner.determineLeafSortOn(new TriplePattern("?s", "http://ex/p", "http://ex/o"))); // subject first
        assertEquals("?p", QueryPlanner.determineLeafSortOn(new TriplePattern("http://ex/s", "?p", "http://ex/o"))); // then predicate
        assertEquals("?o", QueryPlanner.determineLeafSortOn(new TriplePattern("http://ex/s", "http://ex/p", "?o"))); // then object
        assertNull(QueryPlanner.determineLeafSortOn(new TriplePattern("http://ex/s", "http://ex/p", "http://ex/o"))); // none
    }

    @Test
    void patternVars_collectsAllVarsPresent() {
        var tp = new TriplePattern("?s", "http://ex/p", "?o");
        Set<String> vars = QueryPlanner.patternVars(tp);
        assertEquals(Set.of("?s", "?o"), vars);
    }

    @Test
    void buildJoinTreeOverRows_prefersJoinOnSharedVar() {
        // Leaf A: vars {?x}, size 10
        QueryPlanner.Node A = new QueryPlanner.Node();
        A.isLeaf = true;
        A.vars = new HashSet<>(Set.of("?x"));
        A.estSize = 10;

        // Leaf B: vars {?x, ?y}, size 100
        QueryPlanner.Node B = new QueryPlanner.Node();
        B.isLeaf = true;
        B.vars = new HashSet<>(Set.of("?x", "?y"));
        B.estSize = 100;

        // Leaf C: vars {?z}, size 5  (shares nothing with A,B)
        QueryPlanner.Node C = new QueryPlanner.Node();
        C.isLeaf = true;
        C.vars = new HashSet<>(Set.of("?z"));
        C.estSize = 5;


        QueryPlanner.Node root = QueryPlanner.buildJoinTreeOverRows(List.of(A, B, C));
        assertNotNull(root);

        // one of the children should be a join of A and B with joinVar "?x"
        QueryPlanner.Node left = root.left;
        QueryPlanner.Node right = root.right;
        QueryPlanner.Node ab = (left != null && !left.isLeaf && left.joinVar != null) ? left :
                (right != null && !right.isLeaf && right.joinVar != null) ? right : null;

        assertNotNull(ab, "Expected a first join on a shared var");
        assertEquals("?x", ab.joinVar);
    }

    @Test
    void buildJoinTreeOverRows_crossJoinsTwoSmallestIfNoSharedVars() {
        QueryPlanner.Node A = new QueryPlanner.Node();
        A.isLeaf = true; A.vars = Set.of("?a"); A.estSize = 20;

        QueryPlanner.Node B = new QueryPlanner.Node();
        B.isLeaf = true; B.vars = Set.of("?b"); B.estSize = 10;

        QueryPlanner.Node C = new QueryPlanner.Node();
        C.isLeaf = true; C.vars = Set.of("?c"); C.estSize = 30;

        // No leaf shares variables; expect first join is between the two smallest (B:10, A:20)
        QueryPlanner.Node root = QueryPlanner.buildJoinTreeOverRows(List.of(A, B, C));
        assertNotNull(root);

        // Find the first join node (the one that doesn't share a var)
        QueryPlanner.Node firstJoin =
                (!root.isLeaf && root.joinVar == null && (root.left.isLeaf || root.right.isLeaf)) ? root :
                        (!root.left.isLeaf && root.left.joinVar == null) ? root.left :
                                (!root.right.isLeaf && root.right.joinVar == null) ? root.right : null;

        assertNotNull(firstJoin, "Expected a cross-join somewhere in the tree");
        assertNull(firstJoin.joinVar, "Cross-join should have null joinVar");
        assertTrue(firstJoin.estSize >= 200, "Estimated size should be product of the two chosen children");
    }
}

