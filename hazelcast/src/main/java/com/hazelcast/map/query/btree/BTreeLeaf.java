package com.hazelcast.map.query.btree;

import static com.hazelcast.map.query.btree.BTree.SYNCHRONIZATION_APPROACH;

public class BTreeLeaf<V> extends NodeBase {

    static final int MAX_ENTRIES_LEAF = 64; // (PAGE_SIZE - PAGE_HEADER_SIZE)/(KEY_POINTER_SIZE + VALUE_POINTER_SIZE);

    static class Entry<K extends Comparable<K>, V> {
        K k;
        V p;
    }

    protected Comparable[] keys = new Comparable[MAX_ENTRIES_LEAF];
    protected V[] payloads = (V[]) new Object[MAX_ENTRIES_LEAF];

    BTreeLeaf() {
        super(PageType.BTREE_LEAF, 0, SYNCHRONIZATION_APPROACH);
        //System.out.println("New leaf page " + this);
    }

    boolean isFull() {
        return count == MAX_ENTRIES_LEAF;
    }

    int lowerBound(Comparable k) {
        int lower = 0;
        int upper = count;
        do {
            int mid = ((upper - lower) / 2) + lower;
            if (k.compareTo(keys[mid]) < 0) {
                upper = mid;
            } else if (k.compareTo(keys[mid]) > 0) {
                lower = mid + 1;
            } else {
                return mid;
            }
        } while (lower < upper);
        return lower;
    }

    V insert(Comparable k, V p) {
        assert count < MAX_ENTRIES_LEAF;
        if (count > 0) {
            int pos = lowerBound(k);
            if ((pos < count) && (keys[pos].equals(k))) {
                // Upsert
                V oldValue = payloads[pos];
                payloads[pos] = p;
                return oldValue;
            }
            System.arraycopy(keys, pos, keys, pos + 1, count - pos);
            System.arraycopy(payloads, pos, payloads, pos + 1, count - pos);
            keys[pos] = k;
            payloads[pos] = p;
        } else {
            keys[0] = k;
            payloads[0] = p;
        }
        count++;
        return null;
    }

    V remove(Comparable k) {
        assert count <= MAX_ENTRIES_LEAF;
        V oldValue = null;
        if (count > 0) {
            int pos = lowerBound(k);
            if (pos < count && keys[pos].equals(k)) {
                // Found a match
                oldValue = payloads[pos];
                System.arraycopy(keys, pos + 1, keys, pos, count - pos - 1);
                System.arraycopy(payloads, pos + 1, payloads, pos, count - pos - 1);
            }
            count--;
        }
        return oldValue;
    }

    BTreeLeaf split() {
        BTreeLeaf newLeaf = new BTreeLeaf();
        newLeaf.count = count-(count/2);
        count = count-newLeaf.count;
        System.arraycopy(keys, count, newLeaf.keys, 0, newLeaf.count);
        System.arraycopy(payloads, count, newLeaf.payloads, 0, newLeaf.count);
        return newLeaf;
    }

    Comparable getSeparatorAfterSplit() {
        return keys[count-1];
    }


}
