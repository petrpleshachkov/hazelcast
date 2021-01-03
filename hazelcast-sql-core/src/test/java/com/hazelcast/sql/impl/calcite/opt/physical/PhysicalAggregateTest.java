package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.opt.logical.AggregateLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.aggregate.AggregatePhysicalRel;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhysicalAggregateTest extends OptimizerTestSupport {

    @Test
    public void testSimpleGroupBy() {
        assertPlan(
            optimizePhysical("SELECT f0 FROM p GROUP BY f0"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 10d),
                planRow(1, AggregatePhysicalRel.class, "group=[{0}]", 10d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[0]]]]", 100d)
            )
        );
    }

}
