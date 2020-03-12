package com.hazelcast.map.query.btree;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.BetweenPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;

import static com.hazelcast.config.IndexType.SORTED;
import static com.hazelcast.config.IndexType.SORTED_BTREE;
import static org.junit.Assert.assertEquals;

public class BTreeQueryTest extends HazelcastTestSupport {

    @Test
    public void testBtreeIndex() throws InterruptedException {
        Config config = getConfig();

        HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Integer, Person> map = node.getMap(randomMapName());

        map.addIndex(SORTED, "name");
        map.addIndex(SORTED, "age");

        // put some data
        for (int i = 0; i < 1000; ++i) {
            map.put(i, new Person(i));
        }

        Predicate p = new EqualPredicate("name", 100);
        assertEquals(1, map.values(p).size());
        p = new BetweenPredicate("age", 100, 200);
        assertEquals(101, map.values(p).size());
    }

    static class Person implements Serializable {

        public final Integer age;
        public final String name;

        Person(Integer value) {
            this.age = value;
            this.name = value.toString();
        }

        public Integer getAge() {
            return age;
        }

        public String getName() {
            return name;
        }
    }

}