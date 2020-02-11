package com.hazelcast.map.query.btree;

import org.apache.commons.lang3.mutable.MutableBoolean;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.map.query.btree.BTreeLeaf.MAX_ENTRIES_LEAF;
import static com.hazelcast.map.query.btree.NodeBase.LockType;
import static com.hazelcast.map.query.btree.NodeBase.LockType.*;
import static com.hazelcast.map.query.btree.NodeBase.PageType.BTREE_INNER;

public class BTree<V> implements BTreeIf<V> {

    static final LockType SYNCHRONIZATION_APPROACH = PESSIMISTIC;

    private volatile NodeBase root;

    public BTree() {
        root = new BTreeLeaf<V>();
    }

    public NodeBase getRoot() {
        return root;
    }

    void makeRoot(Comparable k, NodeBase leftChild, NodeBase rightChild) {
        BTreeInner inner = new BTreeInner();
        inner.count = 1;
        inner.level = leftChild.level + 1;
        inner.keys[0] = k;
        inner.children[0] = leftChild;
        inner.children[1] = rightChild;
        root = inner;
    }

    static void yield(int count) {
        Thread.yield();
    }

    @Override
    public V insert(Comparable k, V v) {
        return insertInternal(k, v);
        //checkRootNodeSingleThread();
    }

    private void checkRootNodeSingleThread() {
        if (!root.checkLockReleased()) {
            new Throwable().printStackTrace(System.err);
            throw new IllegalStateException();
        }
    }

    V insertInternal(Comparable k, V v) {
        int restartCount = 0;
        MutableBoolean needRestart = new MutableBoolean(false);

        restart:
        for (; ; ) {
            if (restartCount++ > 0) {
                //System.out.println("Restarted insert count " + restartCount);
                yield(restartCount);
            }
            needRestart.setValue(false);


            // Current node
            NodeBase node = root;
            long versionNode = node.readLockOrRestart(needRestart);
            if (needRestart.booleanValue() || (node != root)) {
                assert SYNCHRONIZATION_APPROACH == OPTIMISTIC || needRestart.booleanValue() == false;
                node.readUnlockOrRestart(versionNode, needRestart);
                //System.out.println("Insert restart 1");
                continue restart;
            }

            // Parent of current node
            BTreeInner parent = null;
            long versionParent = -1L;

            while (node.type == BTREE_INNER) {
                BTreeInner inner = (BTreeInner) node;

                // Split eagerly if full
                if (inner.isFull()) {
                    // Lock
                    if (parent != null) {
                        versionParent = parent.upgradeToWriteLockOrRestart(versionParent, needRestart);
                        if (needRestart.booleanValue()) {
                            parent.readUnlockOrRestart(versionParent, needRestart);
                            node.readUnlockOrRestart(versionNode, needRestart);
                            //System.out.println("Insert restart 2");
                            continue restart;
                        }
                    }
                    versionNode = node.upgradeToWriteLockOrRestart(versionNode, needRestart);
                    if (needRestart.booleanValue()) {
                        if (parent != null) {
                            parent.writeUnlock();
                        }
                        node.readUnlockOrRestart(versionNode, needRestart);
                        //System.out.println("Insert restart 3");
                        continue restart;
                    }
                    if (parent == null && (node != root)) { // there's a new parent
                        node.writeUnlock();
                        //System.out.println("Insert restart 4");
                        continue restart;
                    }
                    // Split

                    BTreeInner newInner = inner.split();
                    Comparable sep = inner.getSeparatorAfterSplit();
                    //System.out.println("Splitted inner page");

                    if (parent != null) {
                        parent.insert(sep, newInner);
                    } else {
                        makeRoot(sep, inner, newInner);
                    }
                    // Unlock and restart
                    node.writeUnlock();
                    if (parent != null) {
                        parent.writeUnlock();
                    }
                    //System.out.println("Insert restart 5");
                    continue restart;
                }

                if (parent != null) {
                    parent.readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart.booleanValue()) {
                        //System.out.println("Insert restart 6");
                        continue restart;
                    }
                }

                parent = inner;
                versionParent = versionNode;

                node = inner.children[inner.lowerBound(k)];
                inner.checkOrRestart(versionNode, needRestart);
                if (needRestart.booleanValue()) {
                    //System.out.println("Insert restart 7");
                    continue restart;
                }
                versionNode = node.readLockOrRestart(needRestart);
                if (needRestart.booleanValue()) {
                    //System.out.println("Insert restart 8");
                    continue restart;
                }
            }

            BTreeLeaf<V> leaf = (BTreeLeaf) node;

            // Split leaf if full
            if (leaf.count == MAX_ENTRIES_LEAF) {
                // Lock
                if (parent != null) {
                    versionParent = parent.upgradeToWriteLockOrRestart(versionParent, needRestart);
                    if (needRestart.booleanValue()) {
                        parent.readUnlockOrRestart(versionParent, needRestart);
                        node.readUnlockOrRestart(versionNode, needRestart);
                        //System.out.println("Split leaf restart 1");
                        continue restart;
                    }
                }

                versionNode = node.upgradeToWriteLockOrRestart(versionNode, needRestart);
                if (needRestart.booleanValue()) {
                    if (parent != null) {
                        parent.writeUnlock();
                    }
                    node.readUnlockOrRestart(versionNode, needRestart);
                    //System.out.println("Split leaf restart 2");
                    continue restart;
                }
                if (parent == null && (node != root)) { // there's a new parent
                    node.writeUnlock();
                    //System.out.println("Split leaf restart 3");
                    continue restart;
                }
                // Split
                BTreeLeaf newLeaf = leaf.split();
                Comparable sep = leaf.getSeparatorAfterSplit();
                //System.out.println("Splitted leaf page");
                if (parent != null) {
                    parent.insert(sep, newLeaf);
                } else {
                    makeRoot(sep, leaf, newLeaf);
                }
                // Unlock and restart
                node.writeUnlock();
                if (parent != null) {
                    parent.writeUnlock();
                }
                //System.out.println("Splitted leaf page");
                continue restart;
            } else {
                // only lock leaf node
                versionNode = node.upgradeToWriteLockOrRestart(versionNode, needRestart);
                if (needRestart.booleanValue()) {
                    if (parent != null) {
                        parent.readUnlockOrRestart(versionParent, needRestart);
                    }
                    node.readUnlockOrRestart(versionNode, needRestart);
                    //System.out.println("Insert restart 9");
                    continue restart;
                }
                if (parent != null) {
                    parent.readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart.booleanValue()) {
                        node.writeUnlock();
                        //System.out.println("Insert restart 10");
                        continue restart;
                    }
                }
                V oldValue = leaf.insert(k, v);
                node.writeUnlock();
                return oldValue; // success
            }
        }
    }


    @Override
    public V remove(Comparable k) {
        return removeInternal(k);
    }

    @Override
    public void clear() {
        int restartCount = 0;
        MutableBoolean needRestart = new MutableBoolean(false);

        restart:
        for (; ; ) {
            if (restartCount++ > 0) {
                //System.out.println("Restarted remove count " + restartCount);
                yield(restartCount);
            }
            needRestart.setValue(false);

            // Current node
            NodeBase node = root;
            node.writeLockOrRestart(needRestart);
            if (needRestart.booleanValue() || (node != root)) {
                assert SYNCHRONIZATION_APPROACH == OPTIMISTIC || needRestart.booleanValue() == false;
                node.writeUnlock();
                continue restart;
            }

            root = new BTreeLeaf<V>();
            node.writeUnlock();
            return;
        }
    }

    private V removeInternal(Comparable k) {
        int restartCount = 0;
        MutableBoolean needRestart = new MutableBoolean(false);

        restart:
        for (; ; ) {
            if (restartCount++ > 0) {
                //System.out.println("Restarted remove count " + restartCount);
                yield(restartCount);
            }
            needRestart.setValue(false);

            // Current node
            NodeBase node = root;
            long versionNode = node.readLockOrRestart(needRestart);
            if (needRestart.booleanValue() || (node != root)) {
                assert SYNCHRONIZATION_APPROACH == OPTIMISTIC || needRestart.booleanValue() == false;
                node.readUnlockOrRestart(versionNode, needRestart);
                //System.out.println("Insert restart 1");
                continue restart;
            }

            // Parent of current node
            BTreeInner parent = null;
            long versionParent = -1L;

            while (node.type == BTREE_INNER) {
                BTreeInner inner = (BTreeInner) node;

                if (parent != null) {
                    parent.readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart.booleanValue()) {
                        //System.out.println("Insert restart 6");
                        continue restart;
                    }
                }

                parent = inner;
                versionParent = versionNode;

                node = inner.children[inner.lowerBound(k)];
                inner.checkOrRestart(versionNode, needRestart);
                if (needRestart.booleanValue()) {
                    //System.out.println("Insert restart 7");
                    continue restart;
                }
                versionNode = node.readLockOrRestart(needRestart);
                if (needRestart.booleanValue()) {
                    //System.out.println("Insert restart 8");
                    continue restart;
                }
            }

            BTreeLeaf<V> leaf = (BTreeLeaf) node;

            // only lock leaf node
            versionNode = node.upgradeToWriteLockOrRestart(versionNode, needRestart);
            if (needRestart.booleanValue()) {
                if (parent != null) {
                    parent.readUnlockOrRestart(versionParent, needRestart);
                }
                node.readUnlockOrRestart(versionNode, needRestart);
                //System.out.println("Insert restart 9");
                continue restart;
            }
            if (parent != null) {
                parent.readUnlockOrRestart(versionParent, needRestart);
                if (needRestart.booleanValue()) {
                    node.writeUnlock();
                    //System.out.println("Insert restart 10");
                    continue restart;
                }
            }
            boolean emptyLeaf = false;
            V oldValue = leaf.remove(k);
            if (leaf.count == 0) {
                emptyLeaf = true;
            }
            node.writeUnlock();
            if (emptyLeaf) {
                deleteNodeFromBTree(k);
            }
            return oldValue;
        }
    }

    private void deleteNodeFromBTree(Comparable k) {
        int restartCount = 0;
        MutableBoolean needRestart = new MutableBoolean(false);

        restart:
        for (; ; ) {
            if (restartCount++ > 0) {
                //System.out.println("Restarted remove count " + restartCount);
                yield(restartCount);
            }
            needRestart.setValue(false);

            // Current node
            NodeBase node = root;

            long versionNode = node.readLockOrRestart(needRestart);
            if (needRestart.booleanValue() || (node != root)) {
                assert SYNCHRONIZATION_APPROACH == OPTIMISTIC || needRestart.booleanValue() == false;
                node.readUnlockOrRestart(versionNode, needRestart);
                //System.out.println("Insert restart 1");
                continue restart;
            }

            int rootLevel = node.level;
            if (rootLevel == 0) {
                // never delete root node
                node.readUnlockOrRestart(versionNode, needRestart);
                return;
            }

            if (rootLevel == 1) {
                versionNode = node.upgradeToWriteLockOrRestart(versionNode, needRestart);
                if (!needRestart.booleanValue()) {
                    deleteNodeFromParentWriteLocked(k, node, (BTreeInner) node, null, needRestart);
                    return;
                } else {
                    // release read lock on lock and try write lock
                    node.readUnlockOrRestart(versionNode, needRestart);
                    node = root;
                    node.writeLockOrRestart(needRestart);
                    if (node != root) {
                        node.writeUnlock();
                        continue restart;
                    }
                    if (node.level == 1) {
                        deleteNodeFromParentWriteLocked(k, node, (BTreeInner) node, null, needRestart);
                        return;
                    }

                }

            }

            // Parent of current node
            long versionParent = -1L;
            NodeBase ancestor = node;
            List<NodeBase> internals = null;

            do {
                BTreeInner inner = (BTreeInner) node;

                node = inner.children[inner.lowerBound(k)];

                versionNode = node.readLockOrRestart(needRestart);

                if (node.count == 0) {
                    if (internals == null) {
                        internals = new ArrayList<>();
                    }
                    internals.add(node);
                } else {
                    ancestor.readUnlockOrRestart(-1, needRestart);
                    releaseLocks(internals, needRestart);
                    internals = null;
                    ancestor = node;
                }
            } while (node.type == BTREE_INNER && node.level > 1);

            versionNode = ancestor.upgradeToWriteLockOrRestart(versionNode, needRestart);
            if (needRestart.booleanValue()) {
                // TODO instant write lock
                ancestor.readUnlockOrRestart(-1, needRestart);
                releaseLocks(internals, needRestart);
                continue restart;
            } else {
                deleteNodeFromParentWriteLocked(k, node, (BTreeInner) ancestor, internals, needRestart);
                return;
            }
        }
    }

    private void releaseLocks(List<NodeBase> nodes, MutableBoolean needRestart) {
        if (nodes == null) {
            return;
        }
        for (NodeBase node : nodes) {
            node.readUnlockOrRestart(-1, needRestart);
        }
    }

    private void deleteNodeFromParentWriteLocked(Comparable k, NodeBase parent, BTreeInner ancestor, List<NodeBase> internals,
                                                 MutableBoolean needRestart) {
        BTreeInner parentInner = (BTreeInner) parent;

        int keyPos = parentInner.lowerBound(k);
        NodeBase child = parentInner.children[keyPos];

        child.readLockOrRestart(needRestart);
        // TODO: restart logic for optimistic locking

        if (child.count == 0) {
            if (ancestor.count != 0) {
                // the page is still empty, delete it from the ancestor;
                // the deleted subtree will be GCed,
                // including not released locks
                ancestor.remove(k);
                //System.out.println("Removed key " + k + " from inner node " + ancestor + " with level " + ancestor.level );
            }
        }
        child.readUnlockOrRestart(-1, needRestart);
        releaseLocks(internals, needRestart);
        ancestor.writeUnlock();

    }

    @Override
    public V lookup(Comparable k) {
        int restartCount = 0;
        MutableBoolean needRestart = new MutableBoolean(false);

        restart:
        for (; ; ) {
            if (restartCount++ > 0) {
                yield(restartCount);
            }
            needRestart.setValue(false);

            NodeBase node = root;
            long versionNode = node.readLockOrRestart(needRestart);
            assert SYNCHRONIZATION_APPROACH == OPTIMISTIC || needRestart.booleanValue() == false;
            if (needRestart.booleanValue() || (node != root)) {
                //System.out.println("Search restart 1");
                node.readUnlockOrRestart(versionNode, needRestart);
                continue restart;
            }

            // Parent of current node
            BTreeInner parent = null;
            long versionParent = -1L; // not valid

            while (node.type == BTREE_INNER) {
                BTreeInner inner = (BTreeInner) node;

                if (parent != null) {
                    parent.readUnlockOrRestart(versionParent, needRestart);
                    assert SYNCHRONIZATION_APPROACH == OPTIMISTIC || needRestart.booleanValue() == false;
                    if (needRestart.booleanValue()) {
                        //System.out.println("Search restart 2");
                        continue restart;
                    }
                }

                parent = inner;
                versionParent = versionNode;

                int pos = inner.lowerBound(k);
                node = inner.children[inner.lowerBound(k)];
                inner.checkOrRestart(versionNode, needRestart);
                if (needRestart.booleanValue()) {
                    assert SYNCHRONIZATION_APPROACH == OPTIMISTIC || needRestart.booleanValue() == false;
                    if (parent != null) {
                        parent.readUnlockOrRestart(versionParent, needRestart);
                    }
                    //System.out.println("Search restart 3");
                    continue restart;
                }
                versionNode = node.readLockOrRestart(needRestart);
                if (needRestart.booleanValue()) {
                    assert SYNCHRONIZATION_APPROACH == OPTIMISTIC || needRestart.booleanValue() == false;
                    if (parent != null) {
                        parent.readUnlockOrRestart(versionParent, needRestart);
                    }
                    //System.out.println("Search restart 4");
                    continue restart;
                }
            }

            BTreeLeaf leaf = (BTreeLeaf) node;
            int pos = leaf.lowerBound(k);
            V result = null;
            if ((pos < leaf.count) && (leaf.keys[pos].equals(k))) {
                result = (V) leaf.payloads[pos];
            }
            if (parent != null) {
                parent.readUnlockOrRestart(versionParent, needRestart);
                assert SYNCHRONIZATION_APPROACH == OPTIMISTIC || needRestart.booleanValue() == false;
                if (needRestart.booleanValue()) {
                    //System.out.println("Search restart 5");
                    continue restart;
                }
            }
            node.readUnlockOrRestart(versionNode, needRestart);
            assert SYNCHRONIZATION_APPROACH == OPTIMISTIC || needRestart.booleanValue() == false;
            if (needRestart.booleanValue()) {
                //System.out.println("Search restart 6");
                continue restart;
            }

            return result;
        }
    }


    long scan(Comparable k, int range, V[] output) {
        int restartCount = 0;
        MutableBoolean needRestart = new MutableBoolean(false);

        restart:
        for (; ; ) {
            if (restartCount++ > 0) {
                yield(restartCount);
            }
            needRestart.setValue(false);

            NodeBase node = root;
            long versionNode = node.readLockOrRestart(needRestart);
            if (needRestart.booleanValue() || (node != root)) {
                continue restart;
            }

            // Parent of current node
            BTreeInner parent = null;
            long versionParent = -1L;

            while (node.type == BTREE_INNER) {
                BTreeInner inner = (BTreeInner) node;

                if (parent != null) {
                    parent.readUnlockOrRestart(versionParent, needRestart);
                    if (needRestart.booleanValue()) {
                        continue restart;
                    }
                }

                parent = inner;
                versionParent = versionNode;

                node = inner.children[inner.lowerBound(k)];
                inner.checkOrRestart(versionNode, needRestart);
                if (needRestart.booleanValue()) {
                    continue restart;
                }
                versionNode = node.readLockOrRestart(needRestart);
                if (needRestart.booleanValue()) {
                    continue restart;
                }
            }

            BTreeLeaf leaf = (BTreeLeaf) node;
            int pos = leaf.lowerBound(k);
            int count = 0;
            for (int i = pos; i < leaf.count; i++) {
                if (count == range) {
                    break;
                }
                output[count++] = (V) leaf.payloads[i];
            }

            if (parent != null) {
                parent.readUnlockOrRestart(versionParent, needRestart);
                if (needRestart.booleanValue()) {
                    continue restart;
                }
            }
            node.readUnlockOrRestart(versionNode, needRestart);
            if (needRestart.booleanValue()) {
                continue restart;
            }

            return count;
        }
    }

    public static void print(PrintStream out, NodeBase node, BTreeAction action) {
        int restartCount = 0;
        MutableBoolean needRestart = new MutableBoolean(false);

        restart:
        for (; ; ) {
            if (restartCount++ > 0) {
                yield(restartCount);
            }
            needRestart.setValue(false);

            node.writeLockOrRestart(needRestart);
            if (needRestart.booleanValue()) {
                continue restart;
            }

            // Parent of current node
            BTreeInner parent = null;
            long versionParent = -1L; // not valid

            printNode(out, node, action);
            if (node.type == BTREE_INNER) {
                BTreeInner inner = (BTreeInner) node;
                for (int i = 0; i < node.count + 1; ++i) {
                    print(out, inner.children[i], action);
                }
            }

            node.writeUnlock();
            return;
        }
    }

    private static void printNode(PrintStream out, NodeBase node, BTreeAction action) {
        if (node.type == BTREE_INNER) {
            printInnerNode(out, (BTreeInner) node, action);
        } else {
            printLeafNode(out, (BTreeLeaf) node, action);
        }
    }

    private static void printInnerNode(PrintStream out, BTreeInner inner, BTreeAction action) {
        out.println("page type " + inner.type + " keys count " + inner.count);
        for (int i = 0; i < inner.count; ++i) {
            action.onInnerValue(out, inner, inner.keys[i], inner.children[i]);
        }
    }

    private static void printLeafNode(PrintStream out, BTreeLeaf leaf, BTreeAction action) {
        out.println("page type " + leaf.type + " keys count " + leaf.count);
        for (int i = 0; i < leaf.count; ++i) {
            action.onLeafValue(out, leaf, leaf.keys[i], leaf.payloads[i]);
        }
    }

    interface BTreeAction<K extends Comparable<K>, V> {

        void onLeafValue(PrintStream out, NodeBase node, K key, V value);

        void onInnerValue(PrintStream out, NodeBase node, K key, NodeBase child);
    }
}
