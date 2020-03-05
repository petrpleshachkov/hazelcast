package com.hazelcast.map.query.btree;

import com.hazelcast.map.InMemoryFormatTest;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.map.query.btree.BTree.BTreeAction;
import static org.junit.Assert.*;


public class BTreeTest extends HazelcastTestSupport {

    @Test
    public void testLookup() {
        BTree<String> btree = new BTree<>();

        Set<String> keys = new HashSet<>();
        for (int i = 0; i < 10000; ++i) {
            String key = UUID.randomUUID().toString();
            String value = UUID.randomUUID().toString();
            btree.insert(key, value);
            //System.out.println("Inserted key " + key);
            keys.add(key);
        }

        final AtomicInteger keysCount = new AtomicInteger();
        BTree.print(System.out, btree.getRoot(), new BTreeAction() {

            @Override
            public void onLeafValue(PrintStream out, NodeBase node, Comparable key, Object value) {
                out.println("Key " + key + ", value " + value);
                keysCount.addAndGet(1);
            }

            @Override
            public void onInnerValue(PrintStream out, NodeBase node, Comparable key, NodeBase child) {
                out.println("Key " + key + ", child " + child);
            }
        });

        for (String key : keys) {
            assertNotNull(btree.lookup(key));
        }

        System.out.println("keysCount " + keysCount.get());
    }

    @Test
    public void testSequentialIngestBtreeVsSkipList() {
        BTree<String> btree = new BTree<>();

        Set<String> keys = new HashSet<>();
        int entriesNum = 1000000;
        Map<String, String> entries = generateData(entriesNum);


        long start = System.nanoTime();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            btree.insert(entry.getKey(), entry.getValue());
        }
        long btreeNanoTime = System.nanoTime() - start;

        ConcurrentSkipListMap skipListMap = new ConcurrentSkipListMap();
        start = System.nanoTime();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            skipListMap.put(entry.getKey(), entry.getValue());
        }
        long skipListNanoTime = System.nanoTime() - start;

        System.out.println("Put sequentially entriesNum " + entriesNum + " SkipList time " + skipListNanoTime + ", btree time " + btreeNanoTime);
    }

    @Test
    public void testParallelIngestBtreeSkipList() {
        int concurrency = Runtime.getRuntime().availableProcessors();

        BTree<String> btree = new BTree<>();
        ConcurrentSkipListMap skipListMap = new ConcurrentSkipListMap();

        List<Thread> threads = new ArrayList<>();
        List<Map<String, String>> generatedDataForThreads = new ArrayList<>();
        int entriesNumPerThread = 200000;
        for (int i = 0; i < concurrency; ++i) {
            generatedDataForThreads.add(generateData(entriesNumPerThread));
        }

        ConcurrentNavigableMap<String, String> entries = new ConcurrentSkipListMap<>();

        final Random r = new Random();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < concurrency; ++i) {
            final int index = i;
            // inserter
            Thread thread = new Thread(() -> {
                try {
                    Map<String, String> data = generatedDataForThreads.get(index);
                    for (Map.Entry<String, String> entry : data.entrySet()) {
                        btree.insert(entry.getKey(), entry.getValue());
                        //skipListMap.put(entry.getKey(), entry.getValue());
                    }
                    //System.out.println("Finished updater thread");
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                    t.printStackTrace(System.err);
                }
            });
            threads.add(thread);
        }

        long start = System.nanoTime();
        // Start threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Join threads
        for (Thread thread : threads) {
            assertJoinable(20, thread);
        }
        long totalTime = System.nanoTime() - start;

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }
        assertNull(exception.get());

        System.out.println("Total entries num " + concurrency * entriesNumPerThread + ", btree time " + totalTime);
    }

    @Test
    public void testFullScanWithConcurrentUpdates() {
        int concurrency = Runtime.getRuntime().availableProcessors();

        BTree<String> btree = new BTree<>();
        ConcurrentSkipListMap skipListMap = new ConcurrentSkipListMap();

        List<Thread> threads = new ArrayList<>(concurrency);
        List<Thread> scanThreads = new ArrayList<>(concurrency / 2);
        int entriesNum = 1000000;
        Map<String, String> generatedData = generateData(entriesNum);

        for (Map.Entry<String, String> entry : generatedData.entrySet()) {
            //skipListMap.put(entry.getKey(), entry.getValue());
            btree.insert(entry.getKey(), entry.getValue());
        }

        Pair<String, String> pair = getMinMaxKey(generatedData);
        String minKey = pair.getLeft();
        String maxKey = pair.getRight();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < concurrency; ++i) {
            final int index = i;
            Thread thread;
            if (i < concurrency / 2) {
                // Updater
                thread = new Thread(() -> {
                    try {
                        String prevValue = null;
                        for (Map.Entry<String, String> entry : generatedData.entrySet()) {
                            String newValue = prevValue == null ? entry.getValue() : prevValue;
                            //skipListMap.put(entry.getKey(), newValue);
                            //btree.insert(entry.getKey(), entry.getValue());
                            prevValue = entry.getValue();
                        }
                        //System.out.println("Finished updater thread");
                    } catch (Throwable t) {
                        exception.compareAndSet(null, t);
                        t.printStackTrace(System.err);
                    }
                });
            } else {
                // Scanner
                thread = new Thread(() -> {
                    try {
                        //ConcurrentNavigableMap<String, String> subMap = skipListMap.subMap(minKey, true, maxKey, true);
                        //Iterator<Map.Entry<String, String>> it = subMap.entrySet().iterator();
                        ConcurrentIndexValueIterator it = btree.lookup(minKey, true, maxKey, true);
                        int count = 0;
                        while (it.hasNext()) {
                            it.next();
                            ++count;
                        }
                        assertEquals(entriesNum, count);
                    } catch (Throwable t) {
                        exception.compareAndSet(null, t);
                        t.printStackTrace(System.err);
                    }
                });
                scanThreads.add(thread);
            }


            threads.add(thread);
        }

        long start = System.nanoTime();
        // Start threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Join scanner threads
        for (Thread thread : scanThreads) {
            assertJoinable(20, thread);
        }
        long scannerTime = System.nanoTime() - start;

        // Join threads
        for (Thread thread : threads) {
            assertJoinable(20, thread);
        }
        long totalTime = System.nanoTime() - start;

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }
        assertNull(exception.get());

        System.out.println("Total entries num " + entriesNum + ", btree time " + totalTime + ", scan time " + scannerTime);
    }


    @Test
    public void testConcurrentFullScanNoUpdates() {
        int concurrency = Runtime.getRuntime().availableProcessors();

        BTree<String> btree = new BTree<>();
        ConcurrentSkipListMap skipListMap = new ConcurrentSkipListMap();

        List<Thread> scanThreads = new ArrayList<>(concurrency);
        int entriesNum = 1000000;
        Map<String, String> generatedData = generateData(entriesNum);

        for (Map.Entry<String, String> entry : generatedData.entrySet()) {
            //skipListMap.put(entry.getKey(), entry.getValue());
            btree.insert(entry.getKey(), entry.getValue());
        }

        Pair<String, String> pair = getMinMaxKey(generatedData);
        String minKey = pair.getLeft();
        String maxKey = pair.getRight();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLongArray timeArray = new AtomicLongArray(concurrency);
        for (int i = 0; i < concurrency; ++i) {
            Thread thread;
            // Scanner
            final int index = i;
            thread = new Thread(() -> {
                try {
                    long start = System.nanoTime();
                    for (int j = 0; j < 100; ++j) {
                        //ConcurrentNavigableMap<String, String> subMap = skipListMap.subMap(minKey, true, maxKey, true);
                        //Iterator<Map.Entry<String, String>> it = subMap.entrySet().iterator();
                        ConcurrentIndexValueIterator it = btree.lookup(minKey, true, maxKey, true);
                        int count = 0;
                        while (it.hasNext()) {
                            it.next();
                            ++count;
                        }
                        assertEquals(entriesNum, count);
                    }

                    long time = System.nanoTime() - start;
                    timeArray.set(index, time);
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                    t.printStackTrace(System.err);
                }
            });
            scanThreads.add(thread);
        }

        // Start threads
        for (Thread thread : scanThreads) {
            thread.start();
        }

        // Join scanner threads
        for (Thread thread : scanThreads) {
            assertJoinable(200, thread);
        }


        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }

        assertNull(exception.get());

        System.out.println("Total entries num " + entriesNum + ", btree scan time " + timeArray.toString());
    }

    @Test
    public void testConcurrentRandomScanNoUpdates() {
        int concurrency = Runtime.getRuntime().availableProcessors();

        BTree<String> btree = new BTree<>();
        ConcurrentSkipListMap skipListMap = new ConcurrentSkipListMap();

        List<Thread> scanThreads = new ArrayList<>(concurrency);
        int entriesNum = 1000000;
        Map<String, String> generatedData = generateData(entriesNum);
        Set<String> keysSet = new TreeSet<>(generatedData.keySet());
        List<String> keys = new ArrayList<>(keysSet);

        for (Map.Entry<String, String> entry : generatedData.entrySet()) {
            //skipListMap.put(entry.getKey(), entry.getValue());
            btree.insert(entry.getKey(), entry.getValue());
        }

        Random r = new Random();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLongArray timeArray = new AtomicLongArray(concurrency);
        for (int i = 0; i < concurrency; ++i) {
            Thread thread;
            // Scanner
            final int index = i;
            thread = new Thread(() -> {
                try {
                    long start = System.nanoTime();
                    for (int j = 0; j < 100; ++j) {
                        int keyIndex = r.nextInt(keys.size());
                        String minKey = keys.get(keyIndex);
                        String maxKey = (keyIndex + 100) < keys.size() ? keys.get(keyIndex + 100) : keys.get(keys.size() - 1);
                        //ConcurrentNavigableMap<String, String> subMap = skipListMap.subMap(minKey, true, maxKey, true);
                        //Iterator<Map.Entry<String, String>> it = subMap.entrySet().iterator();
                        ConcurrentIndexValueIterator it = btree.lookup(minKey, true, maxKey, true);
                        int count = 0;
                        while (it.hasNext()) {
                            it.next();
                            ++count;
                        }
                        assertTrue(count >= 1);
                    }

                    long time = System.nanoTime() - start;
                    timeArray.set(index, time);
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                    t.printStackTrace(System.err);
                }
            });
            scanThreads.add(thread);
        }

        // Start threads
        for (Thread thread : scanThreads) {
            thread.start();
        }

        // Join scanner threads
        for (Thread thread : scanThreads) {
            assertJoinable(200, thread);
        }


        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }

        assertNull(exception.get());

        System.out.println("Total entries num " + entriesNum + ", skiplist scan time " + timeArray.toString());
    }

    @Test
    public void testConcurrentRandomRemoveInsert() {
        int concurrency = Runtime.getRuntime().availableProcessors();

        BTree<String> btree = new BTree<>();
        ConcurrentSkipListMap<String, String> skipListMap = new ConcurrentSkipListMap();

        List<Thread> scanThreads = new ArrayList<>(concurrency);
        int entriesNum = 1000000;
        Map<String, String> generatedData = generateData(entriesNum);
        Set<String> keysSet = new TreeSet<>(generatedData.keySet());
        List<String> keys = new ArrayList<>(keysSet);

        for (Map.Entry<String, String> entry : generatedData.entrySet()) {
            skipListMap.put(entry.getKey(), entry.getValue());
            //btree.insert(entry.getKey(), entry.getValue());
        }

        Random r = new Random();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLongArray timeArray = new AtomicLongArray(concurrency);
        for (int i = 0; i < concurrency; ++i) {
            Thread thread;
            // Scanner
            final int index = i;
            thread = new Thread(() -> {
                try {
                    long start = System.nanoTime();
                    for (int j = 0; j < 100; ++j) {
                        int keyIndex = r.nextInt(keys.size());

                        //ConcurrentNavigableMap<String, String> subMap = skipListMap.subMap(minKey, true, maxKey, true);
                        //Iterator<Map.Entry<String, String>> it = subMap.entrySet().iterator();
                        String key = keys.get(keyIndex);
                        String value = skipListMap.remove(key);
                        //String value = btree.remove(key);
                        if (value != null) {
                            //btree.insert(key, value);
                            skipListMap.put(key, value);
                        }
                    }

                    long time = System.nanoTime() - start;
                    timeArray.set(index, time);
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                    t.printStackTrace(System.err);
                }
            });
            scanThreads.add(thread);
        }

        // Start threads
        for (Thread thread : scanThreads) {
            thread.start();
        }

        // Join scanner threads
        for (Thread thread : scanThreads) {
            assertJoinable(200, thread);
        }


        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }

        assertNull(exception.get());

        System.out.println("Total entries num " + entriesNum + ", btree remove/insert time " + timeArray.toString());
    }



    @Test
    public void testConcurrentLookupNoUpdates() {
        int concurrency = Runtime.getRuntime().availableProcessors();

        BTree<String> btree = new BTree<>();
        ConcurrentSkipListMap skipListMap = new ConcurrentSkipListMap();

        List<Thread> lookupThreads = new ArrayList<>(concurrency);
        int entriesNum = 1000000;
        Map<String, String> generatedData = generateData(entriesNum);

        for (Map.Entry<String, String> entry : generatedData.entrySet()) {
            //skipListMap.put(entry.getKey(), entry.getValue());
            btree.insert(entry.getKey(), entry.getValue());
        }

        Set<String> keys = generatedData.keySet();

        Pair<String, String> pair = getMinMaxKey(generatedData);
        String minKey = pair.getLeft();
        String maxKey = pair.getRight();

        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < concurrency; ++i) {
            Thread thread;
            // Scanner
            thread = new Thread(() -> {
                try {
                    for (String key : keys) {
                        assertNotNull(btree.lookup(key));
                        //assertNotNull(skipListMap.get(key));
                    }
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                    t.printStackTrace(System.err);
                }
            });
            lookupThreads.add(thread);
        }

        long start = System.nanoTime();
        // Start threads
        for (Thread thread : lookupThreads) {
            thread.start();
        }

        // Join lookup threads
        for (Thread thread : lookupThreads) {
            assertJoinable(200, thread);
        }

        long totalLookupTime = System.nanoTime() - start;

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }

        assertNull(exception.get());

        System.out.println("Total map size " + entriesNum + ", btree total lookup time " + totalLookupTime);
    }


    private Map<String, String> generateData(int count) {
        Map<String, String> entries = new HashMap<>(count);
        for (int i = 0; i < count; ++i) {
            String key = UUID.randomUUID().toString();
            String value = UUID.randomUUID().toString();
            entries.put(key, value);
        }
        return entries;
    }

    Pair<String, String> getMinMaxKey(Map<String, String> map) {
        String min = null;
        String max = null;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (min == null) {
                min = entry.getKey();
                max = entry.getKey();
            } else {
                min = entry.getKey().compareTo(min) < 0 ? entry.getKey() : min;
                max = entry.getKey().compareTo(max) > 0 ? entry.getKey() : max;
            }
        }
        return Pair.of(min, max);
    }


    @Test
    public void testFullScan() {
        BTree<String> btree = new BTree<>();

        String minKey = null;
        String maxKey = null;
        Set<String> keys = new HashSet<>();
        Map<String, String> map = new HashMap();


        int KEYS_NUM = 500000;
        for (int i = 0; i < KEYS_NUM; ++i) {
            String key = UUID.randomUUID().toString();
            String value = UUID.randomUUID().toString();
            btree.insert(key, value);
            //System.out.println("Inserted key " + key);
            keys.add(key);
            if (minKey == null || key.compareTo(minKey) < 0) {
                minKey = key;
            }
            if (maxKey == null || key.compareTo(maxKey) > 0) {
                maxKey = key;
            }
            map.put(key, value);
        }

        ConcurrentIndexValueIterator<String> it = btree.lookup(minKey, true, maxKey, true);
        int count = 0;
        while (it.hasNext()) {
            String key = (String) it.next();
            assertTrue(map.containsKey(key));
            count++;
        }
        assertEquals(KEYS_NUM, count);
    }

    @Test
    public void testInsertRemove() {
        BTree<String> btree = new BTree<>();

        Map<String, String> map = new HashMap();

        for (int n = 0; n < 10; ++n) {
            int count = 0;

            for (int i = 0; i < 300000; ++i) {
                String key = UUID.randomUUID().toString();
                String value = UUID.randomUUID().toString();
                map.put(key, value);

                btree.insert(key, value);
                //System.out.println("Inserted key " + key + " count " + (++count) );

                assertNotNull(btree.lookup(key));
            }

            count = 0;
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                btree.remove(key);
                //System.out.println("Removed key " + key + " count " + (++count) );
                assertNull(btree.lookup(key));
            }

            map.clear();
        }
    }

    @Test
    public void testInsertRemoveMixed() {
        BTree<String> btree = new BTree<>();

        Map<String, String> map = new HashMap();

        for (int n = 0; n < 1000; ++n) {
            for (int i = 0; i < 10000; ++i) {
                String key = UUID.randomUUID().toString();
                String value = UUID.randomUUID().toString();
                map.put(key, value);

                btree.insert(key, value);
                //System.out.println("Inserted key " + key + " count " + (++count) );

                assertNotNull(btree.lookup(key));
            }

            int count = 0;
            Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
            int mapSize = map.size();
            while (it.hasNext()) {
                if (count % 3 >= 1) {
                    Map.Entry<String, String> entry = it.next();
                    String key = entry.getKey();
                    String value = entry.getValue();
                    btree.remove(key);
                    //System.out.println("Removed key " + key + " count " + (++count) );
                    assertNull(btree.lookup(key));
                    it.remove();
                }
                count++;

            }
            assertTrue(map.size() < mapSize);
        }

        System.out.println("Root level " + btree.getRoot().level);
    }

    @Test
    public void testInsertRemoveLookupConcurrently() throws InterruptedException {
        int concurrency = Runtime.getRuntime().availableProcessors();


        BTree<String> btree = new BTree<>();

        List<Thread> threads = new ArrayList<>();
        ConcurrentNavigableMap<String, String> entries = new ConcurrentSkipListMap<>();


        final Random r = new Random();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < concurrency; ++i) {
            Thread thread;
            if (i < concurrency / 2) {
                // inserter
                thread = new Thread(() -> {
                    try {
                        while (!stop.get()) {
                            String key = UUID.randomUUID().toString();
                            String value = UUID.randomUUID().toString();
                            btree.insert(key, value);
                            entries.put(key, value);
                            //System.out.println("Inserted key " + key);
                        }
                        //System.out.println("Finished updater thread");
                    } catch (Throwable t) {
                        exception.compareAndSet(null, t);
                        t.printStackTrace(System.err);
                    }
                });
            } else {

                // remover
                thread = new Thread(() -> {
                    try {
                        while (!stop.get() || !entries.isEmpty()) {

                            String randomKey = UUID.randomUUID().toString();
                            Map.Entry<String, String> entry = entries.floorEntry(randomKey);
                            if (entry == null) {
                                entry = entries.ceilingEntry(randomKey);
                            }

                            if (entry == null) {
                                continue;
                            }

                            if (entries.remove(entry.getKey()) == null) {
                                continue;
                            }
                            btree.remove(entry.getKey());
                            assertNull(btree.lookup(entry.getKey()));
                        }
                    } catch (Exception t) {
                        exception.compareAndSet(null, t);
                        t.printStackTrace(System.err);
                    }
                });
            }
            threads.add(thread);
        }

        // Start threads
        for (Thread thread : threads) {
            thread.start();
        }

        Thread.sleep(10000);
        stop.set(true);

        // Join threads
        for (Thread thread : threads) {
            assertJoinable(10, thread);
        }

        assertTrue(btree.getRoot().count == 0);

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }
        assertNull(exception.get());
    }


    @Test
    public void testConcurrentInsertAndLookup() throws InterruptedException {
        int concurrency = Runtime.getRuntime().availableProcessors();

        BTree<String> btree = new BTree<>();

        List<Thread> threads = new ArrayList<>();
        Map<String, String> entries = new ConcurrentHashMap<>();
        Random r = new Random();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < concurrency; ++i) {
            Thread thread;
            if (i % 3 == 0) {
                // Updater
                thread = new Thread(() -> {
                    try {
                        int keyCount = 0;
                        while (!stop.get() && keyCount < 500000) {
                            String key = UUID.randomUUID().toString();
                            String value = UUID.randomUUID().toString();
                            btree.insert(key, value);
                            entries.put(key, value);
                            keyCount++;
                            //System.out.println("Inserted key " + key);
                        }
                        System.out.println("Finished updater thread");
                    } catch (Throwable t) {
                        exception.compareAndSet(null, t);
                    }
                });
            } else if (i % 3 == 1) {
                // Searcher
                thread = new Thread(() -> {
                    try {
                        int searchCount = 0;
                        while (!stop.get()) {
                            String lookupKey = null;
                            if (lookupKey == null) {
                                String[] keys = entries.keySet().toArray(new String[0]);
                                if (keys.length == 0) {
                                    Thread.yield();
                                    continue;
                                }
                                lookupKey = keys[r.nextInt(keys.length)];
                            }

                            long startTime = System.nanoTime();
                            String lookupValue = btree.lookup(lookupKey);
                            long time = System.nanoTime() - startTime;

                            assertNotNull(lookupValue);
                            searchCount++;
                            //System.out.println("Found key " + lookupKey + " time " + time);
                            if (searchCount % 5 == 0) {
                                lookupKey = null;
                            }
                        }
                        System.out.println("Finished searcher thread, searcheCount " + searchCount);
                    } catch (Exception t) {
                        exception.compareAndSet(null, t);
                    }
                });
            } else {
                // Full scans
                thread = new Thread(() -> {
                    try {
                        int notEmptyscanCount = 0;
                        int minKeysCount = 0;
                        while (!stop.get()) {

                            ConcurrentIndexValueIterator keysIt = btree.lookup(null, true, null, true);

                            int count = 0;
                            while (keysIt.hasNext()) {
                                assertNotNull(btree.lookup(keysIt.next()));
                                count++;
                            }
                            assertNotNull(count >= minKeysCount);
                            minKeysCount = count;
                            if (count > 0) {
                                notEmptyscanCount++;
                            }
                            System.out.println("Scan thread number of keys " + count);
                        }
                        System.out.println("Finished scan thread, scanCount " + notEmptyscanCount);
                    } catch (Exception t) {
                        exception.compareAndSet(null, t);
                        t.printStackTrace(System.err);
                    }
                });

            }
            threads.add(thread);
        }

        // Start threads
        for (Thread thread : threads) {
            thread.start();
        }

        Thread.sleep(10000);
        stop.set(true);

        // Join threads
        for (Thread thread : threads) {
            assertJoinable(thread);
        }


        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }
        assertNull(exception.get());
    }

    @Test
    public void testConcurrentLookup() throws InterruptedException {
        int concurrency = Runtime.getRuntime().availableProcessors() * 4;


        BTree<String> btree = new BTree<>();
        Set<String> keys = new HashSet<>();

        for (int i = 0; i < 500000; ++i) {
            String key = UUID.randomUUID().toString();
            String value = UUID.randomUUID().toString();
            btree.insert(key, value);
            //System.out.println("Inserted key " + key);
            keys.add(key);
        }

        String[] keysArray = keys.toArray(new String[0]);


        List<Thread> threads = new ArrayList<>();
        Map<String, String> entries = new ConcurrentHashMap<>();
        Random r = new Random();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < concurrency; ++i) {
            Thread thread;

            // Searcher
            thread = new Thread(() -> {
                try {
                    int searchCount = 0;
                    while (!stop.get()) {
                        String lookupKey = null;
                        if (lookupKey == null) {
                            if (keysArray.length == 0) {
                                Thread.yield();
                                continue;
                            }
                            lookupKey = keysArray[r.nextInt(keysArray.length)];
                        }

                        long startTime = System.nanoTime();
                        String lookupValue = btree.lookup(lookupKey);
                        long time = System.nanoTime() - startTime;

                        assertNotNull(lookupValue);
                        searchCount++;
                        //System.out.println("Found key " + lookupKey + " time " + time);
                        if (searchCount % 5 == 0) {
                            lookupKey = null;
                        }
                    }
                    System.out.println("Finished searcher thread, searchesCount " + searchCount);
                } catch (Exception t) {
                    exception.compareAndSet(null, t);
                }
            });

            threads.add(thread);
        }

        // Start threads
        for (Thread thread : threads) {
            thread.start();
        }

        Thread.sleep(10000);
        stop.set(true);

        // Join threads
        for (Thread thread : threads) {
            assertJoinable(thread);
        }

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }
        assertNull(exception.get());
    }

    @Test
    public void testLocalIngestVsGlobal() {
        int concurrency = 16; //Runtime.getRuntime().availableProcessors();
        boolean global = true;


        BTree<String> globalBTree = new BTree<>();
        List<BTree<String>> localBTrees = new ArrayList<>(concurrency);
        for (int i = 0; i < concurrency; ++i) {
            localBTrees.add(new BTree<>());
        }

        List<Thread> threads = new ArrayList<>();
        Map<String, String> entries = new ConcurrentHashMap<>();
        Random r = new Random();
        AtomicReference<Throwable> exception = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < concurrency; ++i) {
            Thread thread;
            final int index = i;
            // Updater
            thread = new Thread(() -> {
                try {
                    int keyCount = 0;
                    BTree<String> useBTree = global ? globalBTree : localBTrees.get(index);
                    while (!stop.get() && keyCount < 100000) {
                        String key = new UUID(r.nextLong(), r.nextLong()).toString();
                        String value = new UUID(r.nextLong(), r.nextLong()).toString();
                        useBTree.insert(key, value);
                        entries.put(key, value);
                        keyCount++;
                        //System.out.println("Inserted key " + key);
                    }
                    //System.out.println("Finished updater thread");
                } catch (Throwable t) {
                    exception.compareAndSet(null, t);
                }
            });
            threads.add(thread);
        }


        long start = System.nanoTime();
        // Start threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Join threads
        for (Thread thread : threads) {
            assertJoinable(thread);
        }
        long totalTime = System.nanoTime() - start;

        if (exception.get() != null) {
            exception.get().printStackTrace(System.err);
        }

        assertNull(exception.get());
        System.out.println("Finished ingest, time " + totalTime);
    }

}

