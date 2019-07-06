package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.OrResultSet;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@BinaryInterface
public final class WildcardPredicate extends AbstractIndexAwarePredicate {

    private static String START_END_MARKER = "$";

    private String expression;
    private transient Pattern pattern;

    public WildcardPredicate() {
    }

    public WildcardPredicate(String attribute, String expression) {
        super(attribute);
        this.expression = expression;
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        Index index = getIndex(queryContext);
        return index != null && index.isOrdered() && index.getkgram() > 0;
    }

    @Override
    protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = matchIndex(queryContext, QueryContext.IndexMatchHint.PREFER_UNORDERED);
        Set<Comparable> searchTerms = null;
        for (Comparable kgram : generateKGramsFromQuery(expression, 2, index.getkgram())) {
            Set<Comparable> terms = index.getTerms((String) kgram);
            if (terms == null) {
                return Collections.emptySet();
            }
            if (searchTerms == null) {
                searchTerms = terms;
            } else {
                // make intersection
                searchTerms.retainAll(terms);
            }
        }

        searchTerms = doPostFiltering(searchTerms);

        List<Set<QueryableEntry>> indexedResults = new LinkedList<Set<QueryableEntry>>();
        searchTerms.stream().map(index::getRecords).forEach(indexedResults::add);
        return new OrResultSet(indexedResults);
    }

    private static Set<Comparable> generateKGramsFromQuery(String expression, int minKGmarSize, int maxKGramSize) {
        StringBuilder useExpressionBuilder = new StringBuilder();
        if (!expression.startsWith("*")) {
            // no leading wildcard
            useExpressionBuilder.append("$");
        }
        useExpressionBuilder.append(expression);
        if (!expression.endsWith("*")) {
            // no trailing wildcard
            useExpressionBuilder.append("$");
        }

        String[] terms = useExpressionBuilder.toString().split("\\*");

        if (terms.length == 0) {
            throw new UnsupportedOperationException("Query '" + expression + "' is not supported");
        }

        Set<Comparable> ret = new HashSet<>();
        for (int i = 0; i < terms.length; ++i) {
            int useKgramSize = terms[i].length() > maxKGramSize ? maxKGramSize : terms[i].length();
            generateKGrams(terms[i], useKgramSize, useKgramSize, ret);
        }
        return ret;
    }

    private Set<Comparable> doPostFiltering(Set<Comparable> terms) {
        pattern = pattern != null ? pattern : createPattern(expression);
        return terms.stream().filter(term -> termMatchesExpression(term)).collect(Collectors.toSet());
    }

    private boolean termMatchesExpression(Comparable term) {
        Matcher m = pattern.matcher((String) term);
        return m.matches();
    }

    private static Pattern createPattern(String expression) {
        String regex = expression.replaceAll(".", "[$0]").replace("[*]", ".*");

        return Pattern.compile(regex);
    }

    private static void addNgrams(String value, int nGramSize, Set<Comparable> set) {
        for (int i = 0; i <= value.length() - nGramSize; i++)
            set.add(value.substring(i, i + nGramSize));
    }

    public static void generateKGramsWithStartEndMarker(Comparable value, int minKGmarSize, int maxKGramSize,
                                                        Set<Comparable> ret) {
        if (!(value instanceof String)) {
            throw new UnsupportedOperationException("KGram index is supported for String attribute only");
        }

        String strValue = (String) value;
        if (strValue.isEmpty()) {
            return;
        }
        StringBuilder strBuilder = new StringBuilder(strValue.length() + 2);
        strBuilder.append(START_END_MARKER);
        strBuilder.append(strValue);
        strBuilder.append(START_END_MARKER);

        generateKGrams(strBuilder.toString(), minKGmarSize, maxKGramSize, ret);
    }


    public static void generateKGrams(Comparable value, int minKGmarSize, int maxKGramSize, Set<Comparable> ret) {
        if (!(value instanceof String)) {
            throw new UnsupportedOperationException("KGram index is supported for String attribute only");
        }

        String strValue = (String) value;
        if (strValue.isEmpty()) {
            return;
        }

        for (int size = minKGmarSize; size <= maxKGramSize; size++) {
            addNgrams(strValue, size, ret);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(expression);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        expression = in.readUTF();
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.WILDCARD_PREDICATE;
    }


    @Override
    public String toString() {
        return attributeName + " LIKE '" + expression + "'";
    }

}
