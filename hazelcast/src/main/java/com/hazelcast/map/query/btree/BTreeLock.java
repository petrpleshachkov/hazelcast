package com.hazelcast.map.query.btree;


public interface BTreeLock {

    long readLockOrRestart(MutableBoolean needRestart);

    void readUnlockOrRestart(long startRead, MutableBoolean needRestart);

    long upgradeToWriteLockOrRestart(long version, MutableBoolean needRestart);

    void writeLockOrRestart(MutableBoolean needRestart);

    void instantDurationWriteLock(MutableBoolean needRestart);

    boolean tryWriteLock(MutableBoolean needRestart);

    void writeUnlock();

    void checkOrRestart(long startRead, MutableBoolean needRestart);

    boolean checkLockReleased();
}
