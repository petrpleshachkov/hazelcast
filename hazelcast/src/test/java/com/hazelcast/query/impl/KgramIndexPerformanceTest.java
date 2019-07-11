package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static com.hazelcast.query.SampleTestObjects.Employee;


@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KgramIndexPerformanceTest extends HazelcastTestSupport {

    private IMap<Integer, Employee> map;

    @Before
    public void setUp() {
        Config config = getConfig();
        HazelcastInstance node = createHazelcastInstance(config);
        map = node.getMap("testMap");
        map.addIndex("name", true, 3);
    }

    @Test
    public void testKgramIndexPerformance() {
        int key = 0;
        Random rand = new Random();
        for (int i=0; i < 100000; ++i) {
            Employee employee = new Employee("Fo", rand.nextInt(100), true, rand.nextInt(1000));
            map.put(key++, employee);
        }

        for (int i=0; i < 1000; ++i) {
            Employee employee = new Employee("Fooooo", rand.nextInt(100), true, rand.nextInt(1000));
            map.put(key++, employee);
        }

        for (int i=0; i < 100000; ++i) {
            Employee employee = new Employee("Fob", rand.nextInt(100), true, rand.nextInt(1000));
            map.put(key++, employee);
        }

        for(int i=0; i < 5; ++i) {
            Predicate wildcardPredicate = Predicates.wildcard("name", "Fo*o");

            long startTime = System.currentTimeMillis();
            assertEquals(1000, map.values(wildcardPredicate).size());
            long wildcardQueryTome = System.currentTimeMillis() - startTime;

            System.out.println("===============================================");
            System.out.println("WildcardQuery time " + wildcardQueryTome + "ms");
            System.out.println("===============================================");


            Predicate regexPredicate = Predicates.regex("name", "Fo.*o");
            startTime = System.currentTimeMillis();
            assertEquals(1000, map.values(regexPredicate).size());
            long regexQueryTome = System.currentTimeMillis() - startTime;

            System.out.println("===============================================");
            System.out.println("RegexQuery time " + regexQueryTome + "ms");
            System.out.println("===============================================");

            System.out.println("\n\n\n");
        }
    }
}

