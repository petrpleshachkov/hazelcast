package com.hazelcast.map.query.btree;

import static com.hazelcast.map.query.btree.BTree.SYNCHRONIZATION_APPROACH;
import static com.hazelcast.map.query.btree.NodeBase.PageType.BTREE_INNER;

public class BTreeInner extends NodeBase {

    static final int MAX_ENTRIES_INNER =64;// (PAGE_SIZE - PAGE_HEADER_SIZE) / (KEY_POINTER_SIZE + VALUE_POINTER_SIZE);

    protected NodeBase[] children = new NodeBase[MAX_ENTRIES_INNER];

    protected Comparable[] keys = new Comparable[MAX_ENTRIES_INNER];


    BTreeInner() {
        super(BTREE_INNER, 0, SYNCHRONIZATION_APPROACH);
        //System.out.println("New inner page " + this);
    }

    boolean isFull() {
        return count == (MAX_ENTRIES_INNER - 1);
    }


    int lowerBound(Comparable k) {
        if (k == null) {
            return 0;
        }
        int lower = 0;
        int upper = count;
        while (lower < upper) {
            int mid = ((upper - lower) / 2) + lower;
            if (k.compareTo(keys[mid]) < 0) {
                upper = mid;
            } else if (k.compareTo(keys[mid]) > 0) {
                lower = mid + 1;
            } else {
                return mid;
            }
        }
        return lower;
    }

    BTreeInner split() {
        BTreeInner newInner = new BTreeInner();
        newInner.count = count - (count / 2);
        newInner.level = level;
        count = count - newInner.count - 1;
        System.arraycopy(keys, count + 1, newInner.keys, 0, newInner.count + 1);
        System.arraycopy(children, count + 1, newInner.children, 0, newInner.count + 1);
        return newInner;
    }

    Comparable getSeparatorAfterSplit() {
        return keys[count];
    }


    void insert(Comparable k, NodeBase child) {
        assert count < MAX_ENTRIES_INNER - 1;
        int pos = lowerBound(k);
        System.arraycopy(keys, pos, keys, pos + 1, count - pos + 1);
        System.arraycopy(children, pos, children, pos + 1, count - pos + 1);
        keys[pos] = k;
        children[pos] = children[pos + 1];
        children[pos + 1] = child;
        count++;
    }

    void remove(Comparable k) {
        assert count >= 1; // there is at least one key
        int pos = lowerBound(k);

        // GC
        keys[pos] = null;
        children[pos] = null;

        if (count > 1 && (pos + 1) < count) {
            assert (count - pos - 1) >= 0;
            System.arraycopy(keys, pos + 1, keys, pos, count - pos - 1);
        }

        if (pos < count) {
            assert (count - pos - 1) >= 0;
            System.arraycopy(children, pos + 1, children, pos, count - pos);
        }
        count--;
        assert children[0] != null;
    }

    void clearChild() {
        assert count == 0;
        children[0] = null;
    }
}
