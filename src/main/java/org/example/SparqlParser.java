package org.example;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparqlParser {

    public static class ParsedQuery {
        public List<String> selectVars;
        public boolean distinct;
        public List<TriplePattern> patterns;
        public int limit;

        public ParsedQuery(List<String> selectVars, boolean distinct, List<TriplePattern> patterns, int limit) {
            this.selectVars = selectVars;
            this.distinct = distinct;
            this.patterns = patterns;
            this.limit = limit;
        }
        
    }

    public static class TriplePattern {
        public String subject;
        public String predicate;
        public String object;

        public TriplePattern(String s, String p, String o) {
            this.subject = s;
            this.predicate = p;
            this.object = o;
        }

        @Override
        public String toString() {
            return subject + " " + predicate + " " + object;
        }
    }

    private static String resolvePrefixed(String token, Map<String, String> prefixMap) {
        // if it's a literal string, returns as is
        if (token.startsWith("\"") && token.endsWith("\"")) {
            return token.substring(1, token.length() - 1);
        }

        if (token.startsWith("<") && token.endsWith(">")) {
            return token.substring(1, token.length() - 1);
        }

        if (token.startsWith("?")) {
            return token;
        }

        if (token.contains(":")) {
            String[] parts = token.split(":", 2);
            if (parts.length == 2) {
                String prefix = parts[0];
                String local = parts[1];
                String baseUri = prefixMap.get(prefix);
                if (baseUri != null) {
                    return baseUri + local;
                } else {
                    throw new IllegalArgumentException("Unknown prefix: " + prefix);
                }
            }
        }

        return token;
    }


    private static List<String> expandSemicolonSyntax(String wherePart) {
        List<String> expandedPatterns = new ArrayList<>();

        // Split by periods first to handle complete statements
        String[] statements = wherePart.split("\\s*\\.\\s*(?=\\s|$)");

        for (String statement : statements) {
            statement = statement.trim();
            if (statement.isEmpty()) continue;

            // Check if this statement contains semicolons
            if (statement.contains(";")) {
                expandedPatterns.addAll(expandSemicolonStatement(statement));
            } else {
                expandedPatterns.add(statement);
            }
        }

        return expandedPatterns;
    }

    private static List<String> expandSemicolonStatement(String statement) {
        List<String> patterns = new ArrayList<>();

        // Pattern to match the first triple (subject predicate object)
        Pattern firstTriplePattern = Pattern.compile(
                "^\\s*(<[^>]+>|\\?[a-zA-Z_][a-zA-Z0-9_]*|\\w+:\\w+)\\s+" +
                        "(<[^>]+>|\\?[a-zA-Z_][a-zA-Z0-9_]*|\\w+:\\w+)\\s+" +
                        "(<[^>]+>|\"[^\"]*\"|\\?[a-zA-Z_][a-zA-Z0-9_]*|\\w+:\\w+)\\s*"
        );

        Matcher firstMatcher = firstTriplePattern.matcher(statement);
        if (!firstMatcher.find()) {
            throw new IllegalArgumentException("Invalid statement with semicolon: " + statement);
        }

        String subject = firstMatcher.group(1).trim();
        String firstPredicate = firstMatcher.group(2).trim();
        String firstObject = firstMatcher.group(3).trim();

        patterns.add(subject + " " + firstPredicate + " " + firstObject);

        String remainder = statement.substring(firstMatcher.end()).trim();

        String[] predicateObjectPairs = remainder.split("\\s*;\\s*");

        for (String pair : predicateObjectPairs) {
            pair = pair.trim();
            if (pair.isEmpty()) continue;

            Pattern poPattern = Pattern.compile(
                    "^\\s*(<[^>]+>|\\?[a-zA-Z_][a-zA-Z0-9_]*|\\w+:\\w+)\\s+" +
                            "(<[^>]+>|\"[^\"]*\"|\\?[a-zA-Z_][a-zA-Z0-9_]*|\\w+:\\w+)\\s*"
            );

            Matcher poMatcher = poPattern.matcher(pair);
            if (!poMatcher.matches()) {
                throw new IllegalArgumentException("Invalid predicate-object pair: " + pair);
            }

            String predicate = poMatcher.group(1).trim();
            String object = poMatcher.group(2).trim();

            patterns.add(subject + " " + predicate + " " + object);
        }

        return patterns;
    }

    public static ParsedQuery parse(String query) {
        query = query.trim();
        Map<String, String> prefixMap = new HashMap<>();

        // parses prefix declarations
        Pattern prefixPattern = Pattern.compile("(?i)PREFIX\\s+(\\w+):\\s*<([^>]+)>\\s*");
        Matcher m = prefixPattern.matcher(query);
        while (m.find()) {
            prefixMap.put(m.group(1), m.group(2));
        }
        query = query.replaceAll("(?i)PREFIX\\s+\\w+:\\s*[^>]+>\\s*", "").trim();

        int limit = -1;
        Pattern limitPattern = Pattern.compile("(?i)LIMIT\\s+(\\d+)\\s*$");
        Matcher limitMatcher = limitPattern.matcher(query);
        if (limitMatcher.find()) {
            try {
                limit = Integer.parseInt(limitMatcher.group(1));
                if (limit < 0) {
                    throw new IllegalArgumentException("Limit must be a positive integer");
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Limit must be a positive integer");
            }
            query = query.substring(0, limitMatcher.start()).trim();
        }

        // checks basic SELECT ... WHERE { ... }
        if (!query.toUpperCase().contains("SELECT") || !query.toUpperCase().contains("WHERE")) {
            throw new IllegalArgumentException("Invalid query: " + query);
        }

        // extract SELECT part and detect DISTINCT
        String[] parts = query.split("(?i)WHERE");
        String selectPart = parts[0].replaceFirst("(?i)SELECT", "").trim();
        boolean distinct = false;
        if (selectPart.toUpperCase().startsWith("DISTINCT")) {
            distinct = true;
            selectPart = selectPart.substring(8).trim(); // remove DISTINCT
        }

        // extract SELECT vars
        List<String> selectVars = new ArrayList<>();
        if (!selectPart.isEmpty()) {
            selectVars = Arrays.asList(selectPart.split("\\s+"));
        }

        // extract WHERE patterns
        String wherePart = parts[1].trim();
        if (!wherePart.startsWith("{") || !wherePart.endsWith("}")) {
            throw new IllegalArgumentException("Invalid WHERE clause: " + wherePart);
        }

        wherePart = wherePart.substring(1, wherePart.length() - 1).trim();

        List<String> patternStrings = expandSemicolonSyntax(wherePart);

        List<TriplePattern> patterns = new ArrayList<>();
        Pattern triplePattern = Pattern.compile(
                "(<[^>]+>|\\?[a-zA-Z_][a-zA-Z0-9_]*|\\w+:\\w+)\\s+" +
                        "(<[^>]+>|\\?[a-zA-Z_][a-zA-Z0-9_]*|\\w+:\\w+)\\s+" +
                        "(<[^>]+>|\"[^\"]*\"|\\?[a-zA-Z_][a-zA-Z0-9_]*|\\w+:\\w+)"
        );

        for (String ptn : patternStrings) {
            ptn = ptn.trim();
            if (ptn.isEmpty()) continue;

            Matcher matcher = triplePattern.matcher(ptn);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid triple pattern: " + ptn);
            }
            String subj = resolvePrefixed(matcher.group(1), prefixMap);
            String pred = resolvePrefixed(matcher.group(2), prefixMap);
            String obj = resolvePrefixed(matcher.group(3), prefixMap);
            patterns.add(new TriplePattern(subj, pred, obj));
        }

        return new ParsedQuery(selectVars, distinct, patterns, limit);
    }
}