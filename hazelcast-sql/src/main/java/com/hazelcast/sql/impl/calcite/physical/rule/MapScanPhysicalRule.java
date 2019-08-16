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

package com.hazelcast.sql.impl.calcite.physical.rule;

import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.rel.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.PhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.ReplicatedMapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;

public class MapScanPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new MapScanPhysicalRule();

    private MapScanPhysicalRule() {
        super(
            RuleUtils.single(MapScanLogicalRel.class, HazelcastConventions.LOGICAL),
            MapScanPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MapScanLogicalRel scan = call.rel(0);

        RelOptTable table = scan.getTable();

        HazelcastTable hazelcastTable = table.unwrap(HazelcastTable.class);

        PhysicalRel newScan;

        if (hazelcastTable.isReplicated()) {
            newScan = new ReplicatedMapScanPhysicalRel(
                scan.getCluster(),
                RuleUtils.toPhysicalConvention(scan.getTraitSet(), PhysicalDistributionTrait.REPLICATED),
                table,
                scan.deriveRowType()
            );
        }
        else {
            newScan = new MapScanPhysicalRel(
                scan.getCluster(),
                RuleUtils.toPhysicalConvention(scan.getTraitSet(), PhysicalDistributionTrait.PARTITIONED),
                table,
                scan.deriveRowType()
            );
        }

        call.transformTo(newScan);
    }
}