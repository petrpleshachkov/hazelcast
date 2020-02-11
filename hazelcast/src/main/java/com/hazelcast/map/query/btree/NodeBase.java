package com.hazelcast.map.query.btree;

import org.apache.commons.lang3.mutable.MutableBoolean;

import java.lang.reflect.Array;

import static com.hazelcast.map.query.btree.NodeBase.LockType.OPTIMISTIC;
import static com.hazelcast.map.query.btree.NodeBase.LockType.SPIN;

class NodeBase implements BTreeLock {

    static final int PAGE_SIZE = 8 * 1024;
    static final int PAGE_HEADER_SIZE = 8;
    static final int KEY_POINTER_SIZE = 8;
    static final int VALUE_POINTER_SIZE = 8;

    public enum PageType {
        BTREE_INNER,
        BTREE_LEAF
    }

    public enum LockType {
        OPTIMISTIC,
        PESSIMISTIC,
        SPIN
    }

    protected final PageType type;
    protected int count;
    protected int level;
    protected BTreeLock lock;
    protected long lockAddr;

    NodeBase(PageType type, int count, LockType lockType) {
        this.type = type;
        this.count = count;

        if (lockType == OPTIMISTIC) {
            this.lock = new OptBTreeLock();
        } else if (lockType == SPIN) {
            this.lock = new SpinBTreeLock(128).init(1);
        } else {
            this.lock = new PessimBTreeLock();
        }
    }

    public static <T> T[] createArray(Class<T> type, int size) {
        return (T[]) Array.newInstance(type, size);
    }

    @Override
    public long readLockOrRestart(MutableBoolean needRestart) {
        return lock.readLockOrRestart(needRestart);
    }

    @Override
    public void readUnlockOrRestart(long startRead, MutableBoolean needRestart) {
        lock.readUnlockOrRestart(startRead, needRestart);
    }

    @Override
    public long upgradeToWriteLockOrRestart(long version, MutableBoolean needRestart) {
        return lock.upgradeToWriteLockOrRestart(version, needRestart);
    }

    @Override
    public void writeLockOrRestart(MutableBoolean needRestart) {
        lock.writeLockOrRestart(needRestart);
    }

    @Override
    public void writeUnlock() {
        lock.writeUnlock();
    }

    @Override
    public void checkOrRestart(long startRead, MutableBoolean needRestart) {
        lock.checkOrRestart(startRead, needRestart);
    }

    @Override
    public boolean checkLockReleased() {
        return lock.checkLockReleased();
    }
}
