/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.physical.aggregate;

import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTraitDef;
import com.hazelcast.sql.impl.calcite.opt.logical.AggregateLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.SortLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.UnicastExchangePhysicalRel;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Rule for physical aggregate optimization.
 */
public final class AggregatePhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new AggregatePhysicalRule();

    private AggregatePhysicalRule() {
        super(
            OptUtils.parentChild(AggregateLogicalRel.class, RelNode.class, HazelcastConventions.LOGICAL),
            AggregatePhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        AggregateLogicalRel logicalAgg = call.rel(0);
        RelNode input = logicalAgg.getInput();

        Collection<RelNode> physicalInputs = OptUtils.getPhysicalRelsFromSubset(OptUtils.toPhysicalInput(input));

        if (physicalInputs.isEmpty()) {
            // TODO: Create separate rule for pessimistic optimization, while this rule should listen to non-optimized physical
            //  aggregates (that is, with ANY distribution), and convert them to concrete form with propagated distribution.
            return;
        }

        Collection<RelNode> transforms = getTransforms(logicalAgg, physicalInputs);

        for (RelNode transform : transforms) {
            call.transformTo(transform);
        }
    }

    private Collection<RelNode> getTransforms(AggregateLogicalRel logicalAgg, Collection<RelNode> physicalInputs) {
        List<RelNode> localSortedRels = new ArrayList<>(1);
        List<RelNode> noLocallySortRels = new ArrayList<>(1);

        for (RelNode physicalInput : physicalInputs) {
            RelCollation inputCollation = physicalInput.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
            boolean locallySorted = hasLocalSortForAggregate(logicalAgg.getGroupSet(), inputCollation);
            RelNode newAgg = optimize(logicalAgg, physicalInput);

            if (locallySorted) {
                localSortedRels.add(newAgg);
            } else {
                noLocallySortRels.add(newAgg);
            }
        }
        if (localSortedRels.size() > 0) {
            return localSortedRels;
        } else {
            return noLocallySortRels;
        }

    }


    /**
     * Create an aggregate from the logical aggregate for the given physical input.
     *
     * @param logicalAgg    Logical aggregate.
     * @param physicalInput Physical input.
     * @return Physical aggregate.
     */
    private static RelNode optimize(AggregateLogicalRel logicalAgg, RelNode physicalInput) {
        // 1. Get collation which will be applied to the local part of the aggregate.
        AggregateCollation localAggCollation = AggregateCollation.of(logicalAgg, physicalInput);

        // 2. Get distribution and collocation info.
        AggregateDistribution localAggDistribution = AggregateDistribution.of(logicalAgg, physicalInput);

        // 3. Fast path for collocated aggregate: create single-step aggregation.
        if (localAggDistribution.isCollocated()) {
            RelTraitSet traitSet = OptUtils.traitPlus(
                physicalInput.getTraitSet(),
                localAggCollation.getCollation(),
                localAggDistribution.getDistribution()
            );

            return new AggregatePhysicalRel(
                logicalAgg.getCluster(),
                traitSet,
                physicalInput,
                logicalAgg.getGroupSet(),
                logicalAgg.getGroupSets(),
                logicalAgg.getAggCallList(),
                localAggCollation.getSortedGroupSet()
            );
        }

        // 4. Otherwise, convert to two-phase aggregate.
        return optimizeNonCollocated(logicalAgg, physicalInput, localAggCollation, localAggDistribution.getDistribution());
    }

    /**
     * Create physical representation of a non-collocated aggregate.
     *
     * @param logicalAgg           Logical aggregate.
     * @param physicalInput        Physical input.
     * @param localAggCollation    Collation of a local aggregate.
     * @param localAggDistribution Distribution of a local aggregate.
     * @return Two-step aggregate.
     */
    // TODO: We loose collation of a distributed counterpart at the moment for the sake of prototype simplicity. Make sure to
    //  preserve sorting with help of exchanges with sort-merge semantics.
    private static RelNode optimizeNonCollocated(
        AggregateLogicalRel logicalAgg,
        RelNode physicalInput,
        AggregateCollation localAggCollation,
        DistributionTrait localAggDistribution
    ) {
        // 1. Split aggregate functions.
        AggregateCallSplit split = AggregateCallSplit.of(
            logicalAgg.getCluster(),
            logicalAgg.getGroupCount(),
            logicalAgg.getAggCallList()
        );

        // 2. Prepare local aggregate.
        HazelcastRelOptCluster cluster = OptUtils.getCluster(logicalAgg);
        DistributionTraitDef distributionTraitDef = cluster.getDistributionTraitDef();

        RelTraitSet localTraitSet = OptUtils.traitPlus(
            physicalInput.getTraitSet(),
            localAggCollation.getCollation(),
            localAggDistribution
        );

        AggregatePhysicalRel localAgg = new AggregatePhysicalRel(
            cluster,
            localTraitSet,
            physicalInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            split.getLocalCalls(),
            localAggCollation.getSortedGroupSet()
        );

        // TODO check
        assert logicalAgg.getGroupCount() > 0;

        List<Integer> hashFields = localAgg.getGroupSet().toList();

        DistributionTrait distribution = distributionTraitDef.createPartitionedTrait(hashFields);

        RelNode exchange = new UnicastExchangePhysicalRel(
            cluster,
            OptUtils.toPhysicalConvention(cluster.traitSet(), distribution),
            localAgg,
            hashFields
        );

        // Second phase aggregate
        return new AggregatePhysicalRel(
            cluster,
            exchange.getTraitSet(),
            exchange,
            logicalAgg.getGroupSet(),
            localAgg.getGroupSets(),
            split.getDistributedCalls(),
            ImmutableBitSet.of()
        );
    }

    private static boolean hasLocalSortForAggregate(ImmutableBitSet aggGroupSet, RelCollation inputCollation) {
        if (aggGroupSet.length() > inputCollation.getFieldCollations().size()) {
            return false;
        }

        for (int i = 0; i < aggGroupSet.length(); ++i) {
            RelFieldCollation fieldCollation = inputCollation.getFieldCollations().get(i);
            if (!aggGroupSet.get(fieldCollation.getFieldIndex())) {
                return false;
            }
        }

        return true;
    }

}