package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.json.RandomPrint;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.util.Random;

import static com.hazelcast.instance.impl.TestUtil.toData;
import static org.junit.Assert.assertEquals;
import static com.hazelcast.query.SampleTestObjects.Employee;

@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KgramIndexTest extends HazelcastTestSupport {

    private final InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private Indexes indexes;
    IMap<Integer, SampleTestObjects.Employee> map;
/*
    @Before
    public void setUp() {
        indexes = Indexes.newBuilder(serializationService, IndexCopyBehavior.COPY_ON_READ).build();
        indexes.addOrGetIndex("name", true, 3, null);
    }*/


    @Before
    public void setUp() {
        Config config = getConfig();
        HazelcastInstance node = createHazelcastInstance(config);
        map = node.getMap("testMap");
        map.addIndex("name", true, 3);

    }

    @Test
    public void testKgramIndex() {

        SampleTestObjects.Employee employee1 = new SampleTestObjects.Employee("Foo", 50, true, 100);
        indexes.putEntry(new QueryEntry(serializationService, toData(1), employee1, newExtractor()), null,
                    Index.OperationSource.USER);
        SampleTestObjects.Employee employee2 = new SampleTestObjects.Employee("FooBar", 50, true, 100);
        indexes.putEntry(new QueryEntry(serializationService, toData(2), employee2, newExtractor()), null,
                Index.OperationSource.USER);
        SampleTestObjects.Employee employee3 = new SampleTestObjects.Employee("FooFooBar", 50, true, 100);
        indexes.putEntry(new QueryEntry(serializationService, toData(3), employee3, newExtractor()), null,
                Index.OperationSource.USER);

        Predicate predicate1 = Predicates.wildcard("name", "Foo*");
        assertEquals(3, indexes.query(predicate1).size());

        Predicate predicate2 = Predicates.wildcard("name", "Foo*Bar");
        assertEquals(2, indexes.query(predicate2).size());

        Predicate predicate3 = Predicates.wildcard("name", "*Bar");
        assertEquals(2, indexes.query(predicate3).size());

        Predicate predicate4 = Predicates.wildcard("name", "*oo*");
        assertEquals(3, indexes.query(predicate4).size());

        Predicate predicate5 = Predicates.wildcard("name", "*oF*");
        assertEquals(1, indexes.query(predicate5).size());

        Predicate predicate6 = Predicates.wildcard("name", "*");
        assertThrows(UnsupportedOperationException.class, () -> indexes.query(predicate6));

        Predicate predicate7 = Predicates.wildcard("name", "F*ooBa*");
        assertEquals(2, indexes.query(predicate7).size());

        Predicate predicate8 = Predicates.wildcard("name", "*ooBa*rr");
        assertEquals(0, indexes.query(predicate8).size());

        // TODO: check post filetering, red*, retired
    }

    @Test
    public void testEmptyResult() {
        // TODO check the performance if there is no result
    }

    private Extractors newExtractor() {
        return Extractors.newBuilder(serializationService).build();
    }


}
