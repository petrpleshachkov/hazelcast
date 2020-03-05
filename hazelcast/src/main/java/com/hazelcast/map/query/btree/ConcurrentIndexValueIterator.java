package com.hazelcast.map.query.btree;

import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ConcurrentIndexValueIterator<V> implements Iterator<Comparable> {

    private final BTree<V> btree;
    private final Comparable to;
    private final boolean toInclusive;
    private Comparable lastKey;
    protected Comparable nextKey;
    protected V lastValue;
    protected V nextValue;
    protected BTreeLeaf<V> currentNode;
    protected int currentKeyPos;
    protected long sequenceNumber;

    ConcurrentIndexValueIterator(BTree<V> btree, Comparable to, boolean toInclusive) {
        this.btree = btree;
        this.to = to;
        this.toInclusive = toInclusive;
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
        lastValue = nextValue;
        nextKey = null;
        nextValue = null;
        return lastKey;
    }

    public V value() {
        return lastValue;
    }

    void nextKey() {
        MutableBoolean needRestart = new MutableBoolean();
        boolean readKeyOpt = false;
        boolean skipOptLock = true;
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

                // Try optimistically
                if (!skipOptLock && sequenceNumber == currentNode.sequenceNumber.get()) {
                    // Page hasn't changed
                    readKeyOpt = true;
                    break;
                }


                currentNode.readLockOrRestart(needRestart);
                if (sequenceNumber == currentNode.sequenceNumber.get()) {
                    // Page hasn't changed
                    break;
                }
                /* The page has changed since the previous key. Find the current key again. */
                currentNode.readUnlockOrRestart(-1, needRestart);
                ConcurrentIndexValueIterator<V> it = btree.lookup(lastKey, true, null, true);

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
                    nextValue = it.nextValue;
                    nextKeyIsWithinRange();
                    return;
                }
            }
            Comparable key;
            V value;
            do {
                if (currentKeyPos >= currentNode.count - 1) {

                    if (readKeyOpt) {
                        skipOptLock = true;
                        readKeyOpt = false;
                        continue restart;
                    }


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
                        sequenceNumber = currentNode.sequenceNumber.get();
                        currentKeyPos = 0;
                    } while (currentNode.count == 0);
                } else {
                    currentKeyPos++;
                }

                key = currentNode.keys[currentKeyPos];
                value = currentNode.payloads[currentKeyPos];

                if (readKeyOpt && !skipOptLock && sequenceNumber != currentNode.sequenceNumber.get()) {
                    skipOptLock = true;
                    readKeyOpt = false;
                    continue restart;
                }

            } while (key.equals(lastKey));
            if (!readKeyOpt) {
                currentNode.readUnlockOrRestart(-1, needRestart);
            }
            nextKey = key;
            nextValue = value;
            nextKeyIsWithinRange();
            return;
        }
    }

    boolean nextKeyIsWithinRange() {
        if (to == null) {
            return true; // Nothing to check
        }

        if (nextKey != null &&
                (toInclusive && nextKey.compareTo(to) <= 0
                        || !toInclusive && nextKey.compareTo(to) < 0)) {
            return true;
        }
        // Passed end key
        nextKey = null;
        nextValue = null;
        return false;
    }
}
