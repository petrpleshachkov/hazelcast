package com.hazelcast.map.query.btree;

import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ConcurrentIndexValueIterator<V> implements Iterator<Comparable> {

    private final BTree<V> btree;
    private final Comparable to;
    private Comparable lastKey;
    protected Comparable nextKey;
    private V lastValue;
    protected BTreeLeaf<V> currentNode;
    protected int currentKeyPos;
    protected long sequenceNumber;

    ConcurrentIndexValueIterator(BTree<V> btree, Comparable to) {
        this.btree = btree;
        this.to = to;
    }

    @Override
    public boolean hasNext() {
        if (nextKey != null) {
            return true;
        }
        nextKey();
        return nextKey != null;
    }

    @Override
    public Comparable next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        lastKey = nextKey;
        nextKey = null;
        return lastKey;
    }

    public V value() {
        throw new UnsupportedOperationException();
    }

    void nextKey() {
        MutableBoolean needRestart = new MutableBoolean();
        restart:
        for (; ; ) {
            assert nextKey == null;
            if (lastKey == null) {
                // We already reached the end
                return;
            }
            needRestart.setValue(false);
            byte[] data;
            for (; ; ) {
                currentNode.readLockOrRestart(needRestart);
                if (sequenceNumber == currentNode.sequenceNumber) {
                    // Page hasn't changed
                    break;
                }
                /* The page has changed since the previous key. Find the current key again. */
                currentNode.readUnlockOrRestart(-1, needRestart);
                ConcurrentIndexValueIterator it = btree.lookup(lastKey, null);

                if (it.nextKey == null) {
                    // Key has disappeared and we've reached the end
                    return;
                }
                sequenceNumber = it.sequenceNumber;
                currentNode = it.currentNode;
                currentKeyPos = it.currentKeyPos;
                if (!it.nextKey.equals(lastKey)) {
                    // The last key has disappeared from the index and we've already found the next key
                    nextKey = it.nextKey;
                    nextKeyIsWithinRange();
                    return;
                }
            }
            Comparable key;
            do {
                if (currentKeyPos >= currentNode.count - 1) {
                    /* try next page and skip it if it is empty */
                    do {
                        BTreeLeaf rightChild = currentNode.right;
                        if (rightChild == null) {
                            currentNode.readUnlockOrRestart(-1, needRestart);
                            return;
                        }

                        rightChild.readLockOrRestart(needRestart);
                        assert !needRestart.booleanValue();
                        currentNode.readUnlockOrRestart(-1, needRestart);
                        currentNode = rightChild;
                        sequenceNumber = currentNode.sequenceNumber;
                        currentKeyPos = 0;
                    } while (currentNode.count == 0);
                } else {
                    currentKeyPos++;
                }

                key = currentNode.keys[currentKeyPos];
            } while (key.equals(lastKey));
            currentNode.readUnlockOrRestart(-1, needRestart);
            nextKey = key;
            nextKeyIsWithinRange();
            return;
        }
    }

    boolean nextKeyIsWithinRange() {
        if (to == null) {
            return true; // Nothing to check
        }

        if (nextKey != null && nextKey.compareTo(to) <= 0) {
            return true;
        }
        // Passed end key
        nextKey = null;
        return false;
    }
}
