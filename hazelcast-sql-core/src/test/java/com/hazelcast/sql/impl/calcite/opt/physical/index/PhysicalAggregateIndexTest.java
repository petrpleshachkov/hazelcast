package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.calcite.opt.physical.FilterPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.aggregate.AggregatePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.UnicastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.schema.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhysicalAggregateIndexTest extends IndexOptimizerTestSupport {

    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        HazelcastTable pTable = partitionedTable(
            "p",
            fields("ret", INT, "f1", INT, "f2", INT, "f3", INT, "f4", INT),
            Arrays.asList(
                new MapTableIndex("sorted_f2", IndexType.SORTED, 2, singletonList(2), singletonList(INT)),
                new MapTableIndex("sorted_f1_f3", IndexType.SORTED, 2, asList(1, 3), asList(INT, INT)),
                new MapTableIndex("sorted_f1_f2_f4", IndexType.SORTED, 3, asList(1, 2, 4), asList(INT, INT, INT)),
                new MapTableIndex("sorted_f4_f2_f3", IndexType.SORTED, 3, asList(4, 2, 3), asList(INT, INT, INT)),
                new MapTableIndex("sorted_f1_f2_f3_f4", IndexType.SORTED, 4, asList(1, 2, 3, 4), asList(INT, INT, INT, INT))

            ),
            100,
            false
        );

        tableMap.put("p", pTable);

        return new HazelcastSchema(tableMap);
    }

    @Test
    public void testSimpleGroupBy() {
        assertPlan(
            optimizePhysical("SELECT f2 FROM p GROUP BY f2"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 10d),
                planRow(1, AggregatePhysicalRel.class, "group=[{0}]", 10d),
                planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[2]]]], index=[sorted_f2], indexExp=[null], remainderExp=[null]", 100d)
            )
        );
    }

    @Test
    public void testSimpleGroupByNoIndex() {
        assertPlan(
            optimizePhysical("SELECT f3 FROM p GROUP BY f3"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 10d),
                planRow(1, AggregatePhysicalRel.class, "group=[{0}]", 10d),
                planRow(2, MapScanPhysicalRel.class, "table=[[hazelcast, p[projects=[3]]]]", 100d)
            )
        );
    }

    @Test
    public void testSimpleGroupByManyNodes() {
        assertPlan(
            optimizePhysical("SELECT f2 FROM p GROUP BY f2", 5),
            plan(
                planRow(0, RootPhysicalRel.class, "", 1d),
                planRow(1, RootExchangePhysicalRel.class, "", 1d),
                planRow(2, AggregatePhysicalRel.class, "group=[{0}]", 1d),
                planRow(3, UnicastExchangePhysicalRel.class, "hashFields=[[0]]", 10d),
                planRow(4, AggregatePhysicalRel.class, "group=[{0}]", 10d),
                planRow(5, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[2]]]], index=[sorted_f2], indexExp=[null], remainderExp=[null]", 100d)
            )
        );
    }

    @Test
    public void testGroupByCompositeFields() {
        assertPlan(
            optimizePhysical("SELECT f1, f3 FROM p GROUP BY f1, f3"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 10d),
                planRow(1, AggregatePhysicalRel.class, "group=[{0, 1}]", 10d),
                planRow(2, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
            )
        );
    }

    @Test
    public void testGroupByCompositeFields2() {
        assertPlan(
            optimizePhysical("SELECT f3, f1 FROM p GROUP BY f3, f1, f2"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 10d),
                planRow(1, ProjectPhysicalRel.class, "f3=[$0], f1=[$1]", 10d),
                planRow(2, AggregatePhysicalRel.class, "group=[{0, 1, 2}]", 10d),
                planRow(3, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[3, 1, 2]]]], index=[sorted_f1_f2_f3_f4], indexExp=[null], remainderExp=[null]", 100d)
            )
        );
    }

    @Test
    public void testSimpleGroupBySUM() {
        assertPlan(
            optimizePhysical("SELECT f1, SUM(f1) FROM p GROUP BY f1"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 10d),
                planRow(1, ProjectPhysicalRel.class, "f1=[$0], EXPR$1=[SUM($0)]", 10d),
                planRow(2, AggregatePhysicalRel.class, "group=[{0}]", 10d),
                planRow(3, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
            )
        );
    }

    @Test
    public void testSimpleGroupBySUMManyNodes() {
        assertPlan(
            optimizePhysical("SELECT f1, SUM(f1) FROM p GROUP BY f1", 5),
            plan(
                planRow(0, RootPhysicalRel.class, "", 1d),
                planRow(1, RootExchangePhysicalRel.class, "", 1d),
                planRow(2, ProjectPhysicalRel.class, "f1=[$0], EXPR$1=[SUM($0)]", 1d),
                planRow(3, AggregatePhysicalRel.class, "group=[{0}]", 1d),
                planRow(4, UnicastExchangePhysicalRel.class, "hashFields=[[0]]", 10d),
                planRow(5, AggregatePhysicalRel.class, "group=[{0}]", 10d),
                planRow(6, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1]]]], index=[sorted_f1_f2_f4], indexExp=[null], remainderExp=[null]", 100d)
            )
        );
    }


    @Test
    public void testGroupByTwoAggregates() {
        assertPlan(
            optimizePhysical("SELECT f1, SUM(f1), SUM(f3) FROM p GROUP BY f1, f3"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 10d),
                planRow(1, ProjectPhysicalRel.class, "f1=[$0], EXPR$1=[SUM($0)], EXPR$2=[SUM($1)]", 10d),
                planRow(2, AggregatePhysicalRel.class, "group=[{0, 1}]", 10d),
                planRow(3, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 3]]]], index=[sorted_f1_f3], indexExp=[null], remainderExp=[null]", 100d)
            )
        );
    }

    @Test
    public void testGroupBySUMAndHaving() {
        assertPlan(
            optimizePhysical("SELECT f1, SUM(f2) FROM p GROUP BY f1, f2, f4 HAVING SUM(f4) > 1000"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 5d),
                planRow(1, ProjectPhysicalRel.class, "f1=[$0], EXPR$1=[SUM($1)]", 5d),
                planRow(2, FilterPhysicalRel.class, "condition=[>(SUM($2), 1000)]", 5d),
                planRow(3, AggregatePhysicalRel.class, "group=[{0, 1, 2}]", 10d),
                planRow(4, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 4]]]], index=[sorted_f1_f2_f4], indexExp=[null], remainderExp=[null]", 100d)
            )
        );
    }

    @Test
    public void testGroupBySUMAndHaving2() {
        assertPlan(
            optimizePhysical("SELECT f1, SUM(f2) FROM p GROUP BY f1, f2, f4 HAVING SUM(f1 + f4) > 1000"),
            plan(
                planRow(0, RootPhysicalRel.class, "", 5d),
                planRow(1, ProjectPhysicalRel.class, "f1=[$0], EXPR$1=[SUM($1)]", 5d),
                planRow(2, FilterPhysicalRel.class, "condition=[>(SUM(+(CAST($0):BIGINT(32), CAST($2):BIGINT(32))), 1000)]", 5d),
                planRow(3, AggregatePhysicalRel.class, "group=[{0, 1, 2}]", 10d),
                planRow(4, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 4]]]], index=[sorted_f1_f2_f4], indexExp=[null], remainderExp=[null]", 100d)
            )
        );
    }

    @Test
    public void testGroupBySUMAndHaving2ManyNodes() {
        assertPlan(
            optimizePhysical("SELECT f1, SUM(f2) FROM p GROUP BY f1, f2, f4 HAVING SUM(f1 + f4) > 1000", 5),
            plan(
                planRow(0, RootPhysicalRel.class, "", 1d),
                planRow(1, RootExchangePhysicalRel.class, "", 1d),
                planRow(2, ProjectPhysicalRel.class, "f1=[$0], EXPR$1=[SUM($1)]", 1d),
                planRow(3, FilterPhysicalRel.class, "condition=[>(SUM(+(CAST($0):BIGINT(32), CAST($2):BIGINT(32))), 1000)]", 1d),
                planRow(4, AggregatePhysicalRel.class, "group=[{0, 1, 2}]", 1d),
                planRow(5, UnicastExchangePhysicalRel.class, "hashFields=[[0, 1, 2]]", 10d),
                planRow(6, AggregatePhysicalRel.class, "group=[{0, 1, 2}]", 10d),
                planRow(7, MapIndexScanPhysicalRel.class, "table=[[hazelcast, p[projects=[1, 2, 4]]]], index=[sorted_f1_f2_f4], indexExp=[null], remainderExp=[null]", 100d)
            )
        );
    }


}
