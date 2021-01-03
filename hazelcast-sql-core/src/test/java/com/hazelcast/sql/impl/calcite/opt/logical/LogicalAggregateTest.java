package com.hazelcast.sql.impl.calcite.opt.logical;

import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test for aggregate optimizations.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LogicalAggregateTest extends OptimizerTestSupport {

    @Test
    public void testSimpleGroupBy() {
        assertPlan(
            optimizeLogical("SELECT f0 FROM p GROUP BY f0"),
            plan(
                planRow(0, RootLogicalRel.class, "", 10d),
                planRow(1, AggregateLogicalRel.class, "group=[{0}]", 10d),
                planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[0]]]]", 100d)
            )
        );
    }

    @Test
    public void testGroupByCompositeFields() {
        assertPlan(
            optimizeLogical("SELECT f1, f3 FROM p GROUP BY f1, f3"),
            plan(
                planRow(0, RootLogicalRel.class, "", 10d),
                planRow(1, AggregateLogicalRel.class, "group=[{0, 1}]", 10d),
                planRow(2, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]]", 100d)
            )
        );
    }

    @Test
    public void testSimpleGroupBySUM() {
        assertPlan(
            optimizeLogical("SELECT f1, SUM(f1) FROM p GROUP BY f1"),
            plan(
                planRow(0, RootLogicalRel.class, "", 10d),
                planRow(1, ProjectLogicalRel.class, "f1=[$0], EXPR$1=[SUM($0)]", 10d),
                planRow(2, AggregateLogicalRel.class, "group=[{0}]", 10d),
                planRow(3, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[1]]]]", 100d)
            )
        );
    }

    @Test
    public void testGroupByTwoAggregates() {
        assertPlan(
            optimizeLogical("SELECT f1, SUM(f1), SUM(f2) FROM p GROUP BY f1, f2"),
            plan(
                planRow(0, RootLogicalRel.class, "", 10d),
                planRow(1, ProjectLogicalRel.class, "f1=[$0], EXPR$1=[SUM($0)], EXPR$2=[SUM($1)]", 10d),
                planRow(2, AggregateLogicalRel.class, "group=[{0, 1}]", 10d),
                planRow(3, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[1, 2]]]]", 100d)
            )
        );
    }

    @Test
    public void testGroupBySUMAndHaving() {
        assertPlan(
            optimizeLogical("SELECT f1, SUM(f2) FROM p GROUP BY f1, f2, f4 HAVING SUM(f4) > 1000"),
            plan(
                planRow(0, RootLogicalRel.class, "", 5d),
                planRow(1, ProjectLogicalRel.class, "f1=[$0], EXPR$1=[SUM($1)]", 5d),
                planRow(2, FilterLogicalRel.class, "condition=[>(SUM($2), 1000)]", 5d),
                planRow(3, AggregateLogicalRel.class, "group=[{0, 1, 2}]", 10d),
                planRow(4, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 4]]]]", 100d)
            )
        );
    }

    @Test
    public void testGroupBySUMAndHaving2() {
        assertPlan(
            optimizeLogical("SELECT f1, SUM(f2) FROM p GROUP BY f1, f2, f4 HAVING SUM(f1 + f4) > 1000"),
            plan(
                planRow(0, RootLogicalRel.class, "", 5d),
                planRow(1, ProjectLogicalRel.class, "f1=[$0], EXPR$1=[SUM($1)]", 5d),
                planRow(2, FilterLogicalRel.class, "condition=[>(SUM(+(CAST($0):BIGINT(32), CAST($2):BIGINT(32))), 1000)]", 5d),
                planRow(3, AggregateLogicalRel.class, "group=[{0, 1, 2}]", 10d),
                planRow(4, MapScanLogicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 4]]]]", 100d)
            )
        );
    }


}
