package com.hazelcast.map.query.btree;

import java.lang.reflect.Array;

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
        SPIN,
        NO_SYNC
    }

    protected final PageType type;
    protected int count;
    protected int level;
    protected BTreeLock lock;
    protected long lockAddr;

    NodeBase(PageType type, int count, LockType lockType) {
        this.type = type;
        this.count = count;

        switch(lockType) {
            case OPTIMISTIC:
                this.lock = new OptBTreeLock();
                break;
            case SPIN:
                this.lock = new SpinBTreeLock(128).init(1);
                break;
            case PESSIMISTIC:
                this.lock = new PessimBTreeLock();
                break;
            case NO_SYNC:
                this.lock = new NoSyncLock();
                break;
            default:
                throw new IllegalStateException(lockType.toString());
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
    public void instantDurationWriteLock(MutableBoolean needRestart) {
        lock.instantDurationWriteLock(needRestart);
    }

    @Override
    public boolean tryWriteLock(MutableBoolean needRestart) {
        return lock.tryWriteLock(needRestart);
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
