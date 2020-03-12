package com.hazelcast.map.query.btree;


import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.internal.memory.impl.UnsafeUtil.UNSAFE;

public class SpinBTreeLock implements BTreeLock {


    /**
     * TODO benchmark optimal spin count.
     */
    public static final int SPIN_CNT = 32;

    /**
     *
     */
    public static final boolean USE_RANDOM_RW_POLICY = false;

    /**
     * Always lock tag.
     */
    public static final int TAG_LOCK_ALWAYS = -1;

    /**
     * Lock size.
     */
    public static final int LOCK_SIZE = 8;

    /**
     * Maximum number of waiting threads, read or write.
     */
    public static final int MAX_WAITERS = 0xFFFF;

    /**
     *
     */
    private final ReentrantLock[] locks;

    /**
     *
     */
    private final Condition[] readConditions;

    /**
     *
     */
    private final Condition[] writeConditions;

    /**
     *
     */
    private final AtomicInteger[] balancers;

    /**
     *
     */
    private int monitorsMask;

    /**
     *
     */
    private final AtomicLong onHeapLockState;


    /**
     * @param concLvl Concurrency level, must be a power of two.
     */
    public SpinBTreeLock(int concLvl) {
        onHeapLockState = new AtomicLong();
        if ((concLvl & concLvl - 1) != 0)
            throw new IllegalArgumentException("Concurrency level must be a power of 2: " + concLvl);

        monitorsMask = concLvl - 1;

        locks = new ReentrantLock[concLvl];
        readConditions = new Condition[concLvl];
        writeConditions = new Condition[concLvl];
        balancers = new AtomicInteger[concLvl];

        for (int i = 0; i < locks.length; i++) {
            ReentrantLock lock = new ReentrantLock();

            locks[i] = lock;
            readConditions[i] = lock.newCondition();
            writeConditions[i] = lock.newCondition();
            balancers[i] = new AtomicInteger(0);
        }
    }

    /**
     *
     */
    public SpinBTreeLock init(int tag) {
        tag &= 0xFFFF;

        assert tag != 0;

        onHeapLockState.set((long) tag << 16);
        return this;
    }


    public boolean readLock(int tag) {
        long state = onHeapLockState.get();

        assert state != 0;

        // Check write waiters first.
        int writeWaitCnt = writersWaitCount(state);

        if (writeWaitCnt == 0) {
            for (int i = 0; i < SPIN_CNT; i++) {
                if (!checkTag(state, tag))
                    return false;

                if (canReadLock(state)) {
                    if (onHeapLockState.compareAndSet(state, updateState(state, 1, 0, 0)))
                        return true;
                    else
                        // Retry CAS, do not count as spin cycle.
                        i--;
                }

                state = onHeapLockState.get();
            }
        }

        int idx = lockIndex(onHeapLockState);

        ReentrantLock lockObj = locks[idx];

        lockObj.lock();

        try {

            updateReadersWaitCount(onHeapLockState, lockObj, 1);

            return waitAcquireReadLock(onHeapLockState, idx, tag);
        } finally {
            lockObj.unlock();
        }
    }

    /**
     * @param lock Lock address.
     */
    public void readUnlock(AtomicLong lock) {
        while (true) {
            long state = lock.get();

            if (lockCount(state) <= 0)
                throw new IllegalMonitorStateException("Attempted to release a read lock while not holding it ");

            long updated = updateState(state, -1, 0, 0);

            assert updated != 0;

            if (lock.compareAndSet(state, updated)) {
                // Notify monitor if we were CASed to zero and there is a write waiter.
                if (lockCount(updated) == 0 && writersWaitCount(updated) > 0) {
                    int idx = lockIndex(lock);

                    ReentrantLock lockObj = locks[idx];

                    lockObj.lock();

                    try {
                        // Note that we signal all waiters for this stripe. Since not all waiters in this
                        // stripe/index belong to this particular lock, we can't wake up just one of them.
                        writeConditions[idx].signalAll();
                    } finally {
                        lockObj.unlock();
                    }
                }

                return;
            }
        }
    }

    /**
     * @param lock Lock address.
     */
    public boolean tryWriteLock(AtomicLong lock, int tag) {
        long state = lock.get();

        return checkTag(state, tag) && canWriteLock(state) &&
                lock.compareAndSet(state, updateState(state, -1, 0, 0));
    }

    /**
     * @param lock Lock address.
     */
    public boolean writeLock(AtomicLong lock, int tag) {
        assert tag != 0;

        for (int i = 0; i < SPIN_CNT; i++) {
            long state = lock.get();

            assert state != 0;

            if (!checkTag(state, tag))
                return false;

            if (canWriteLock(state)) {
                if (lock.compareAndSet(state, updateState(state, -1, 0, 0))) {
                    return true;
                } else
                    // Retry CAS, do not count as spin cycle.
                    i--;
            }
        }

        int idx = lockIndex(lock);

        ReentrantLock lockObj = locks[idx];

        lockObj.lock();

        try {
            updateWritersWaitCount(lock, lockObj, 1);

            return waitAcquireWriteLock(lock, idx, tag);
        } finally {
            lockObj.unlock();
        }
    }

    /**
     * @param lock Lock to check.
     * @return {@code True} if write lock is held by any thread for the given offheap RW lock.
     */
    public boolean isWriteLocked(long lock) {
        return lockCount(UNSAFE.getLongVolatile(null, lock)) == -1;
    }

    /**
     * @param lock Lock to check.
     * @return {@code True} if at least one read lock is held by any thread for the given offheap RW lock.
     */
    public boolean isReadLocked(long lock) {
        return lockCount(UNSAFE.getLongVolatile(null, lock)) > 0;
    }

    /**
     * @param lock Lock address.
     */
    public void writeUnlock(AtomicLong lock, int tag) {
        long updated;

        assert tag != 0;

        while (true) {
            long state = lock.get();

            if (lockCount(state) != -1)
                throw new IllegalMonitorStateException("Attempted to release write lock while not holding it ");

            updated = releaseWithTag(state, tag);

            assert updated != 0;

            if (lock.compareAndSet(state, updated))
                break;
        }

        int writeWaitCnt = writersWaitCount(updated);
        int readWaitCnt = readersWaitCount(updated);

        if (writeWaitCnt > 0 || readWaitCnt > 0) {
            int idx = lockIndex(lock);

            ReentrantLock lockObj = locks[idx];

            lockObj.lock();

            try {
                signalNextWaiter(writeWaitCnt, readWaitCnt, idx);
            } finally {
                lockObj.unlock();
            }
        }

    }

    /**
     * @param writeWaitCnt Writers wait count.
     * @param readWaitCnt  Readers wait count.
     * @param idx          Lock index.
     */
    private void signalNextWaiter(int writeWaitCnt, int readWaitCnt, int idx) {
        // Note that we signal all waiters for this stripe. Since not all waiters in this stripe/index belong
        // to this particular lock, we can't wake up just one of them.
        if (writeWaitCnt == 0) {
            Condition readCondition = readConditions[idx];

            readCondition.signalAll();
        } else if (readWaitCnt == 0) {
            Condition writeCond = writeConditions[idx];

            writeCond.signalAll();
        } else {
            // We have both writers and readers.
            if (USE_RANDOM_RW_POLICY) {
                boolean write = (balancers[idx].incrementAndGet() & 0x1) == 0;

                Condition cond = (write ? writeConditions : readConditions)[idx];

                cond.signalAll();
            } else {
                Condition cond = writeConditions[idx];

                cond.signalAll();
            }
        }
    }

    /**
     * Upgrades a read lock to a write lock. If this thread is the only read-owner of the read lock,
     * this method will atomically upgrade the read lock to the write lock. In this case {@code true}
     * will be returned. If not, the read lock will be released and write lock will be acquired, leaving
     * a potential gap for other threads to modify a protected resource. In this case this method will return
     * {@code false}.
     * <p>
     * After this method has been called, there is no need to call to {@link #readUnlock(long)} because
     * read lock will be released in any case.
     *
     * @param lock Lock to upgrade.
     * @return {@code null} if tag validation failed, {@code true} if successfully traded the read lock to
     * the write lock without leaving a gap. Returns {@code false} otherwise, in this case the resource
     * state must be re-validated.
     */
    public Boolean upgradeToWriteLock(AtomicLong lock, int tag) {
        for (int i = 0; i < SPIN_CNT; i++) {
            long state = lock.get();

            if (!checkTag(state, tag))
                return null;

            if (lockCount(state) == 1) {
                if (lock.compareAndSet(state, updateState(state, -2, 0, 0))) {
                    return true;
                } else
                    // Retry CAS, do not count as spin cycle.
                    i--;
            }
        }

        int idx = lockIndex(lock);

        ReentrantLock lockObj = locks[idx];

        lockObj.lock();

        try {
            // First, add write waiter.
            while (true) {
                long state = lock.get();

                if (!checkTag(state, tag))
                    return null;

                if (lockCount(state) == 1) {
                    if (lock.compareAndSet(state, updateState(state, -2, 0, 0)))
                        return true;
                    else
                        continue;
                }

                // Remove read lock and add write waiter simultaneously.
                if (lock.compareAndSet(state, updateState(state, -1, 0, 1)))
                    break;
            }

            return waitAcquireWriteLock(lock, idx, tag);
        } finally {
            lockObj.unlock();
        }
    }

    public Boolean tryUpgradeToWriteLock(AtomicLong lock, int tag) {
        for (int i = 0; i < SPIN_CNT; i++) {
            long state = lock.get();

            if (!checkTag(state, tag))
                return null;

            if (lockCount(state) == 1) {
                if (lock.compareAndSet(state, updateState(state, -2, 0, 0))) {
                    return true;
                } else
                    // Retry CAS, do not count as spin cycle.
                    i--;
            }
        }

        return false;
    }


    /**
     * Acquires read lock in waiting loop.
     *
     * @param lock    Lock address.
     * @param lockIdx Lock index.
     * @param tag     Validation tag.
     * @return {@code True} if lock was acquired, {@code false} if tag validation failed.
     */
    private boolean waitAcquireReadLock(AtomicLong lock, int lockIdx, int tag) {
        ReentrantLock lockObj = locks[lockIdx];
        Condition waitCond = readConditions[lockIdx];

        assert lockObj.isHeldByCurrentThread();

        boolean interrupted = false;

        try {
            while (true) {
                try {
                    long state = lock.get();

                    if (!checkTag(state, tag)) {
                        // We cannot lock with this tag, release waiter.
                        long updated = updateState(state, 0, -1, 0);

                        if (lock.compareAndSet(state, updated)) {
                            int writeWaitCnt = writersWaitCount(updated);
                            int readWaitCnt = readersWaitCount(updated);

                            signalNextWaiter(writeWaitCnt, readWaitCnt, lockIdx);

                            return false;
                        }
                    } else if (canReadLock(state)) {
                        long updated = updateState(state, 1, -1, 0);

                        if (lock.compareAndSet(state, updated))
                            return true;
                    } else
                        waitCond.await();
                } catch (InterruptedException ignore) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * Acquires write lock in waiting loop.
     *
     * @param lock    Lock address.
     * @param lockIdx Lock index.
     * @param tag     Validation tag.
     * @return {@code True} if lock was acquired, {@code false} if tag validation failed.
     */
    private boolean waitAcquireWriteLock(AtomicLong lock, int lockIdx, int tag) {
        ReentrantLock lockObj = locks[lockIdx];
        Condition waitCond = writeConditions[lockIdx];

        assert lockObj.isHeldByCurrentThread();

        boolean interrupted = false;

        try {
            while (true) {
                try {
                    long state = lock.get();

                    if (!checkTag(state, tag)) {
                        // We cannot lock with this tag, release waiter.
                        long updated = updateState(state, 0, 0, -1);

                        if (lock.compareAndSet(state, updated)) {
                            int writeWaitCnt = writersWaitCount(updated);
                            int readWaitCnt = readersWaitCount(updated);

                            signalNextWaiter(writeWaitCnt, readWaitCnt, lockIdx);

                            return false;
                        }
                    } else if (canWriteLock(state)) {
                        long updated = updateState(state, -1, 0, -1);

                        if (lock.compareAndSet(state, updated))
                            return true;
                    } else
                        waitCond.await();
                } catch (InterruptedException ignore) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns index of lock object corresponding to the stripe of this lock address.
     *
     * @param lock Lock address.
     * @return Lock monitor object that corresponds to the stripe for this lock address.
     */
    private int lockIndex(AtomicLong lock) {
        long lockIdentity = System.identityHashCode(lock);
        return safeAbs(hash(lockIdentity)) & monitorsMask;
    }

    /**
     * @param state Lock state.
     * @return {@code True} if write lock is not acquired.
     */
    private boolean canReadLock(long state) {
        return lockCount(state) >= 0;
    }

    /**
     * @param state Lock state.
     * @return {@code True} if no read locks are acquired.
     */
    private boolean canWriteLock(long state) {
        return lockCount(state) == 0;
    }

    /**
     * @param state State.
     * @param tag   Tag.
     */
    private boolean checkTag(long state, int tag) {
        // If passed in tag is negative, lock regardless of the state.
        return tag < 0 || tag(state) == tag;
    }

    /**
     * @param state State.
     * @return Lock count.
     */
    private int lockCount(long state) {
        return (short) (state & 0xFFFF);
    }

    /**
     * @param state Lock state.
     * @return Lock tag.
     */
    private int tag(long state) {
        return (int) ((state >>> 16) & 0xFFFF);
    }

    /**
     * @param state State.
     * @return Writers wait count.
     */
    private int writersWaitCount(long state) {
        return (int) ((state >>> 48) & 0xFFFF);
    }

    /**
     * @param state State.
     * @return Readers wait count.
     */
    private int readersWaitCount(long state) {
        return (int) ((state >>> 32) & 0xFFFF);
    }

    /**
     * @param state            State to update.
     * @param lockDelta        Lock counter delta.
     * @param readersWaitDelta Readers wait delta.
     * @param writersWaitDelta Writers wait delta.
     * @return Modified state.
     */
    private long updateState(long state, int lockDelta, int readersWaitDelta, int writersWaitDelta) {
        int lock = lockCount(state);
        int tag = tag(state);
        int readersWait = readersWaitCount(state);
        int writersWait = writersWaitCount(state);

        lock += lockDelta;
        readersWait += readersWaitDelta;
        writersWait += writersWaitDelta;

        if (readersWait > MAX_WAITERS)
            throw new IllegalStateException("Failed to add read waiter (too many waiting threads): " + MAX_WAITERS);

        if (writersWait > MAX_WAITERS)
            throw new IllegalStateException("Failed to add write waiter (too many waiting threads): " + MAX_WAITERS);

        assert readersWait >= 0 : readersWait;
        assert writersWait >= 0 : writersWait;
        assert lock >= -1;

        return buildState(writersWait, readersWait, tag, lock);
    }

    /**
     * @param state State to update.
     * @return Modified state.
     */
    private long releaseWithTag(long state, int newTag) {
        int lock = lockCount(state);
        int readersWait = readersWaitCount(state);
        int writersWait = writersWaitCount(state);
        int tag = newTag == TAG_LOCK_ALWAYS ? tag(state) : newTag & 0xFFFF;

        lock += 1;

        assert readersWait >= 0 : readersWait;
        assert writersWait >= 0 : writersWait;
        assert lock >= -1;

        return buildState(writersWait, readersWait, tag, lock);
    }

    /**
     * Creates state from counters.
     *
     * @param writersWait Writers wait count.
     * @param readersWait Readers wait count.
     * @param tag         Tag.
     * @param lock        Lock count.
     * @return State.
     */
    private long buildState(int writersWait, int readersWait, int tag, int lock) {
        assert (tag & 0xFFFF0000) == 0;

        return ((long) writersWait << 48) | ((long) readersWait << 32) | ((tag & 0x0000FFFFL) << 16) | (lock & 0xFFFFL);
    }

    /**
     * Updates readers wait count.
     *
     * @param lock  Lock to update.
     * @param delta Delta to update.
     */
    private void updateReadersWaitCount(AtomicLong lock, ReentrantLock lockObj, int delta) {
        assert lockObj.isHeldByCurrentThread();

        while (true) {
            // Safe to do non-volatile read because of CAS below.
            long state = lock.get();

            long updated = updateState(state, 0, delta, 0);

            if (lock.compareAndSet(state, updated))
                return;
        }
    }

    /**
     * Updates writers wait count.
     *
     * @param lock  Lock to update.
     * @param delta Delta to update.
     */
    private void updateWritersWaitCount(AtomicLong lock, ReentrantLock lockObj, int delta) {
        assert lockObj.isHeldByCurrentThread();

        while (true) {
            long state = lock.get();

            long updated = updateState(state, 0, 0, delta);

            if (lock.compareAndSet(state, updated))
                return;
        }
    }

    /**
     * Gets absolute value for integer. If integer is {@link Integer#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Integer.
     * @return Absolute value.
     */
    public static int safeAbs(int i) {
        i = Math.abs(i);

        return i < 0 ? 0 : i;
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     * <p>
     * This function has been taken from Java 8 ConcurrentHashMap with
     * slightly modifications.
     *
     * @param h Value to hash.
     * @return Hash value.
     */
    public static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);

        return h ^ (h >>> 16);
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     * <p>
     * This function has been taken from Java 8 ConcurrentHashMap with
     * slightly modifications.
     *
     * @param key Value to hash.
     * @return Hash value.
     */
    public static int hash(Object key) {
        return hash(key.hashCode());
    }

    /**
     * A primitive override of {@link #hash(Object)} to avoid unnecessary boxing.
     *
     * @param key Value to hash.
     * @return Hash value.
     */
    public static int hash(long key) {
        int val = (int) (key ^ (key >>> 32));

        return hash(val);
    }


    @Override
    public long readLockOrRestart(MutableBoolean needRestart) {
        readLock(-1);
        return 0;
    }

    @Override
    public void readUnlockOrRestart(long startRead, MutableBoolean needRestart) {
        readUnlock(onHeapLockState);
    }

    @Override
    public long upgradeToWriteLockOrRestart(long version, MutableBoolean needRestart) {
        if (!tryUpgradeToWriteLock(onHeapLockState, -1)) {
            needRestart.setValue(true);
        }
        return 0;
    }


    @Override
    public void writeLockOrRestart(MutableBoolean needRestart) {
        writeLock(onHeapLockState, -1);
    }

    @Override
    public boolean tryWriteLock(MutableBoolean needRestart) {
        return tryWriteLock(onHeapLockState, -1);
    }

    @Override
    public void instantDurationWriteLock(MutableBoolean needRestart) {
        if (tryWriteLock(onHeapLockState, -1)) {
            writeUnlock();
            return;
        }
        writeLock(onHeapLockState, -1);
        writeUnlock();
    }


    @Override
    public void writeUnlock() {
        writeUnlock(onHeapLockState, -1);
    }


    @Override
    public void checkOrRestart(long startRead, MutableBoolean needRestart) {
        // no-op
    }

    @Override
    public boolean checkLockReleased() {
        return true;
    }

}
