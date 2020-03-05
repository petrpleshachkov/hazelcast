package com.hazelcast.query.impl;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.query.btree.BTree;
import com.hazelcast.map.query.btree.BTreeIf;
import com.hazelcast.map.query.btree.ConcurrentIndexValueIterator;
import com.hazelcast.query.Predicate;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.query.impl.IndexCopyBehavior.NEVER;

public class BTreeIndexStore extends BaseSingleValueIndexStore {

    private final BTreeIf<QueryableEntry> btreeStore;

    BTreeIndexStore() {
        super(NEVER);
        btreeStore = new BTree();
    }

    @Override
    Object insertInternal(Comparable value, QueryableEntry record) {
        return btreeStore.insert(value, record);
    }

    @Override
    Object removeInternal(Comparable value, Data recordKey) {
        return btreeStore.remove(value);
    }

    @Override
    Comparable canonicalizeScalarForStorage(Comparable value) {
        return value;
    }

    @Override
    public Comparable canonicalizeQueryArgumentScalar(Comparable value) {
        return Comparables.canonicalizeForHashLookup(value);
    }

    @Override
    public void clear() {
        btreeStore.clear();
    }

    @Override
    public boolean isEvaluateOnly() {
        return false;
    }

    @Override
    public boolean canEvaluate(Class<? extends Predicate> predicateClass) {
        return false;
    }

    @Override
    public Set<QueryableEntry> evaluate(Predicate predicate, TypeConverter converter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        assert value != null;
        return Collections.singleton(btreeStore.lookup(value));
    }

    @Override
    public Set<QueryableEntry> getRecords(Set<Comparable> values) {
        Set<QueryableEntry> results = new HashSet<>(values.size());
        for (Comparable value : values) {
            assert value != null;
            results.addAll(getRecords(value));
        }
        return results;
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparison comparison, Comparable value) {
        assert value != null;
        switch (comparison) {
            case LESS:
                return getRecords(null, true, value, false);
            case GREATER:
                return getRecords(value, false, null, true);
            case LESS_OR_EQUAL:
                return getRecords(null, true, value, true);
            case GREATER_OR_EQUAL:
                return getRecords(value, false, null, true);
            default:
                throw new UnsupportedOperationException(comparison.toString());
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive) {
        Set<QueryableEntry> results = new HashSet<>();
        ConcurrentIndexValueIterator<QueryableEntry> it = btreeStore.lookup(from, fromInclusive, to, toInclusive);
        while (it.hasNext()) {
            it.next();
            results.add(it.value());
        }
        return results;
    }
}
