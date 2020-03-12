package com.hazelcast.map.query.btree;

import com.hazelcast.core.HazelcastException;

final class PessimBTreeLock implements BTreeLock {

    private final Object monitor = new Object();
    private int sharedCount;
    private int waitersCount;
    private Throwable writeLockstacktrace;

    @Override
    public long readLockOrRestart(MutableBoolean needRestart) {
        acquireLock(true);
        if (sharedCount > 100) {
            new Throwable().printStackTrace(System.err);
        }
        return 0;
    }

    @Override
    public void readUnlockOrRestart(long startRead, MutableBoolean needRestart) {
        releaseLatch();
    }

    @Override
    public long upgradeToWriteLockOrRestart(long version, MutableBoolean needRestart) {
        if (tryUpgrade()) {
            //writeLockstacktrace = new Throwable();
            return 0;
        } else {
            needRestart.setValue(true);
            return 0;
        }
    }

    boolean tryUpgrade() {
        synchronized (monitor) {
            if (getPageSharedCount() > 1) {
                return false;
            }
            assert getPageSharedCount() == 1;
            setPageSharedCount(-1);
            return true;
        }
    }

    boolean tryWriteLock() {
        synchronized (monitor) {
            if (getPageSharedCount() != 0) {
                return false;
            }
            setPageSharedCount(-1);
        }
        return true;
    }

    @Override
    public void instantDurationWriteLock(MutableBoolean needRestart) {
        boolean interrupted = false;
        synchronized (monitor) {
            while (getPageSharedCount() != 0) {
                try {
                    incPageWaitersCount();
                    monitor.wait();
                } catch (InterruptedException ie) {
                    interrupted = true;
                } finally {
                    decPageWaitersCount();
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void writeLockOrRestart(MutableBoolean needRestart) {
        acquireLock(false);
        //writeLockstacktrace = new Throwable();
    }

    @Override
    public boolean tryWriteLock(MutableBoolean needRestart) {
        return tryWriteLock();
    }

    @Override
    public void writeUnlock() {
        releaseLatch();
        //writeLockstacktrace = null;
    }

    @Override
    public void checkOrRestart(long startRead, MutableBoolean needRestart) {
        // no-op
    }

    @Override
    public boolean checkLockReleased() {
        return sharedCount == 0 && waitersCount == 0;
    }

    private void acquireLock(boolean sharedAccess) {
        synchronized (monitor) {
            while (!isCompatible(sharedAccess)) {
                incPageWaitersCount();
                try {
                    // TODO debug change
                    monitor.wait(3000);
                    if (writeLockstacktrace != null) {
                        writeLockstacktrace.printStackTrace(System.err);
                    }
                    //monitor.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new HazelcastException("Interrupted on acquiring latch on page");
                } finally {
                    decPageWaitersCount();
                }
            }

            // we've got the latch, update the shared counter
            if (sharedAccess) {
                assert getPageSharedCount() >= 0;
                incPageSharedCount();
            } else {
                assert getPageSharedCount() == 0;
                setPageSharedCount(-1);
            }
        }
    }

    private boolean isCompatible(boolean sharedAccess) {
        return sharedAccess ? getPageSharedCount() >= 0 : getPageSharedCount() == 0;
    }


    public void releaseLatch() {
        synchronized (monitor) {
            int sharedCount = getPageSharedCount();
            assert sharedCount != 0;

            if (sharedCount == -1) {
                // we release exclusive lock
                setPageSharedCount(0);
            } else {
                assert sharedCount > 0;
                decPageSharedCount();
            }

            if (getPageSharedCount() == 0 && getPageWaitersCount() > 0) {
                // no users of latch anymore, notify waiters
                monitor.notifyAll();
            }
        }
    }

    void setPageSharedCount(int count) {
        this.sharedCount = count;
    }

    void incPageSharedCount() {
        this.sharedCount++;
    }

    void decPageSharedCount() {
        this.sharedCount--;
    }

    int getPageSharedCount() {
        return sharedCount;
    }

    int getPageWaitersCount() {
        return waitersCount;
    }

    void incPageWaitersCount() {
        this.waitersCount++;
    }

    void decPageWaitersCount() {
        this.waitersCount--;
    }
}
