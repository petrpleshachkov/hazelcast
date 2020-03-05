package com.hazelcast.map.query.btree;

public interface BTreeIf<V> {

    V insert(Comparable k, V v);

    V remove(Comparable k);

    V lookup(Comparable k);

    ConcurrentIndexValueIterator<V> lookup(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive);

    void clear();

}
