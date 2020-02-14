package com.hazelcast.map.query.btree;


import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.concurrent.atomic.AtomicLong;

final class OptBTreeLock implements BTreeLock {

    private AtomicLong typeVersionLockObsolete = new AtomicLong(0b100);

    boolean isLocked(long version) {
        return ((version & 0b10) == 0b10);
    }

    @Override
    public long readLockOrRestart(MutableBoolean needRestart) {
        long version;
        version = typeVersionLockObsolete.get();
        if (isLocked(version)) {
            //Thread.yield();
            needRestart.setValue(true);
        }
        return version;
    }


    @Override
    public long upgradeToWriteLockOrRestart(long version, MutableBoolean needRestart) {
        assert version >= 0L;
        if (typeVersionLockObsolete.compareAndSet(version, version + 0b10)) {
            return version + 0b10;
        } else {
            //Thread.yield();
            needRestart.setValue(true);
        }
        return version;
    }

    @Override
    public void writeLockOrRestart(MutableBoolean needRestart) {
        long version;
        version = readLockOrRestart(needRestart);
        if (needRestart.booleanValue()) {
            return;
        }

        upgradeToWriteLockOrRestart(version, needRestart);
        if (needRestart.booleanValue()) {
            return;
        }
    }

    @Override
    public void writeUnlock() {
        typeVersionLockObsolete.getAndAdd(0b10);
    }

    @Override
    public void checkOrRestart(long startRead, MutableBoolean needRestart) {
        assert startRead >= 0;
        readUnlockOrRestart(startRead, needRestart);
    }

    @Override
    public void readUnlockOrRestart(long startRead, MutableBoolean needRestart) {
        assert startRead >= 0;
        needRestart.setValue(startRead != typeVersionLockObsolete.get());
    }

    @Override
    public boolean checkLockReleased() {
        return true;
    }

    @Override
    public boolean tryWriteLock(MutableBoolean needRestart) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void instantDurationWriteLock(MutableBoolean needRestart) {
        throw new UnsupportedOperationException();
    }
}