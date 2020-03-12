package com.hazelcast.map.query.btree;



public class NoSyncLock implements BTreeLock {

    @Override
    public long readLockOrRestart(MutableBoolean needRestart) {
        return 0;
    }

    @Override
    public void readUnlockOrRestart(long startRead, MutableBoolean needRestart) {

    }

    @Override
    public long upgradeToWriteLockOrRestart(long version, MutableBoolean needRestart) {
        return 0;
    }

    @Override
    public void writeLockOrRestart(MutableBoolean needRestart) {

    }

    @Override
    public void instantDurationWriteLock(MutableBoolean needRestart) {

    }

    @Override
    public boolean tryWriteLock(MutableBoolean needRestart) {
        return true;
    }

    @Override
    public void writeUnlock() {

    }

    @Override
    public void checkOrRestart(long startRead, MutableBoolean needRestart) {

    }

    @Override
    public boolean checkLockReleased() {
        return true;
    }
}
