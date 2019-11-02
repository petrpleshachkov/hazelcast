/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.rel.physical.index;

import com.hazelcast.sql.impl.calcite.cost.CostUtils;
import com.hazelcast.sql.impl.calcite.rel.AbstractScanRel;
import com.hazelcast.sql.impl.calcite.rel.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.PhysicalRelVisitor;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTableIndex;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

/**
 * Map index scan operator.
 */
public class MapIndexScanPhysicalRel extends AbstractScanRel implements PhysicalRel {
    /** Target index. */
    private final HazelcastTableIndex index;

    /** Index filter. */
    private final IndexFilter filter;

    /** Original filter. */
    private final RexNode originalFilter;

    public MapIndexScanPhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<Integer> projects,
        HazelcastTableIndex index,
        IndexFilter filter,
        RexNode originalFilter
    ) {
        super(cluster, traitSet, table, projects);

        this.index = index;
        this.filter = filter;
        this.originalFilter = originalFilter;
    }

    public HazelcastTableIndex getIndex() {
        return index;
    }

    public IndexFilter getFilter() {
        return filter;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MapIndexScanPhysicalRel(getCluster(), traitSet, getTable(), projects, index, filter, originalFilter);
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        visitor.onMapIndexScan(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
           .item("index", index)
           .item("indexFilter", filter.getIndexFilter()).item("remainderFilter", filter.getRemainderFilter());
    }

    // TODO: Dedup with logical scan
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // 1. Get cost of the scan itself. For replicated map cost is multiplied by the number of nodes.
        RelOptCost scanCost = super.computeSelfCost(planner, mq);

        if (table.unwrap(HazelcastTable.class).isReplicated()) {
            scanCost = scanCost.multiplyBy(getHazelcastCluster().getMemberCount());
        }

        // 2. Get cost of the project taking in count filter and number of expressions. Project never produces IO.
        double filterSelectivity = mq.getSelectivity(this, originalFilter);
        double filterRowCount = scanCost.getRows() * filterSelectivity;

        int expressionCount = getProjects().size();

        double projectCpu = CostUtils.adjustProjectCpu(filterRowCount * expressionCount, true);

        // 3. Finally, return sum of both scan and project. Note that we decrease the cost of the scan by selectivity factor.
        RelOptCost totalCost = planner.getCostFactory().makeCost(
            filterRowCount,
            scanCost.getCpu() * filterSelectivity + projectCpu,
            scanCost.getIo()
        );

        return totalCost;
    }
}
