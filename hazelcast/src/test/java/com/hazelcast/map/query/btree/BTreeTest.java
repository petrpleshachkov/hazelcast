package com.hazelcast.map.query.btree;

import com.hazelcast.test.HazelcastTestSupport;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

        ConcurrentIndexValueIterator<String> it = btree.lookup(minKey, maxKey);
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

                            ConcurrentIndexValueIterator keysIt = btree.lookup(null, null);

                            int count = 0;
                            while(keysIt.hasNext()) {
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

