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

package com.hazelcast.sql.impl.exec.aggregate;

import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.expression.aggregate.AggregateExpression;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Executor that performs local-only aggregation. If the input is already sorted properly on the group key, then
 * only a single aggregated row is allocated at a time. Otherwise, the whole result set is consumed from the upstream.
 */
// TODO: Rule to convert aggregate with empty groups to DistinctRel
@SuppressWarnings("rawtypes")
public class AggregateExec extends AbstractUpstreamAwareExec {
    /**
     * Group key.
     */
    private final List<Integer> groupKey;

    /**
     * Expressions.
     */
    // TODO: Use array instead?
    private final List<AggregateExpression> expressions;

    /**
     * Number of columns.
     */
    private final int columnCount;

    /**
     * Current single key (for non-blocking mode).
     */
    private AggregateKey singleKey;

    /**
     * Current single values (for non-blocking mode).
     */
    private List<AggregateCollector> singleValues;

    /**
     * Current row.
     */
    private RowBatch curRow;

    public AggregateExec(
        int id,
        Exec upstream,
        List<Integer> groupKey,
        List<AggregateExpression> expressions,
        int sortedGroupKeySize
    ) {
        super(id, upstream);

        this.groupKey = groupKey;
        this.expressions = expressions;

        // TODO: Currently we only do full sort of the whole key for the sake of simplicty.
        // Throw an exception earlier otherwise
        assert sortedGroupKeySize == groupKey.size();

        columnCount = groupKey.size() + expressions.size();
    }

    @Override
    public IterationResult advance0() {
        while (true) {
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            // Loop through the current batch.
            for (Row upstreamRow : state) {
                // Prepare key and value.
                AggregateKey key = getKey(upstreamRow);
                List<AggregateCollector> values = getValues(key);

                // Accumulate.
                for (int i = 0; i < expressions.size(); i++) {
                    AggregateExpression expression = expressions.get(i);
                    AggregateCollector value = values.get(i);

                    expression.collect(upstreamRow, value, ctx);
                }

                // Special handling of non-blocking mode: if the key has changed, replace old key/value pair with the
                // one, and return the old one as a row.
                if (singleKey == null) {
                    singleKey = key;
                    singleValues = values;
                } else {
                    if (singleKey != key) {
                        curRow = createRowFromKeyAndValues(singleKey, singleValues);

                        singleKey = key;
                        singleValues = values;

                        return IterationResult.FETCHED;
                    }
                }

            }

            // Finalize the state if no more rows are expected.
            if (state.isDone()) {
                if (singleKey == null) {
                    curRow = EmptyRowBatch.INSTANCE;
                } else {
                    curRow = createRowFromKeyAndValues(singleKey, singleValues);

                    singleKey = null;
                    singleValues = null;
                }

                return IterationResult.FETCHED_DONE;
            }
        }
    }

    /**
     * Create the final row from group key and associated values.
     *
     * @param key    Group key.
     * @param values Values.
     * @return Values.
     */
    private HeapRow createRowFromKeyAndValues(AggregateKey key, List<AggregateCollector> values) {
        HeapRow res = new HeapRow(columnCount);

        int idx = 0;

        for (int i = 0; i < key.getCount(); i++) {
            res.set(idx++, key.get(i));
        }

        for (AggregateCollector value : values) {
            res.set(idx++, value.reduce());
        }

        return res;
    }

    /**
     * Get values (collectors) for the given key.
     *
     * @param key Key.
     * @return Values.
     */
    private List<AggregateCollector> getValues(AggregateKey key) {
        if (key == singleKey) {
            return singleValues;
        } else {
            return createValues();
        }
    }

    /**
     * Create new values (aggregators).
     *
     * @return Values.
     */
    private List<AggregateCollector> createValues() {
        int cnt = expressions.size();

        List<AggregateCollector> res = new ArrayList<>(cnt);

        for (AggregateExpression expression : expressions) {
            res.add(expression.newCollector(ctx));
        }

        return res;
    }

    /**
     * Get aggregation key for the given row.
     *
     * @param row Row.
     * @return Aggregation key.
     */
    private AggregateKey getKey(Row row) {
        // In the sorted mode we perform comparison before allocating a new row. If the incoming row matches
        // our expectations, we return already existing group key. Future comparison would be performed by
        // referential equality only.
        if (singleKey != null && singleKey.matches(row)) {
            return singleKey;
        } else {
            return createKey(row);
        }
    }

    /**
     * Create the new key from the row.
     *
     * @param row Row.
     * @return Key.
     */
    private AggregateKey createKey(Row row) {
        int size = groupKey.size();

        switch (size) {
            case 1:
                return AggregateKey.single(row.get(groupKey.get(0)));

            case 2:
                return AggregateKey.dual(row.get(groupKey.get(0)), row.get(groupKey.get(1)));

            default:
                Object[] items = new Object[size];

                for (int i = 0; i < items.length; i++) {
                    items[i] = row.get(groupKey.get(i));
                }

                return AggregateKey.multiple(items);
        }
    }

    @Override
    public RowBatch currentBatch0() {
        return curRow;
    }

    // TODO do we need this?
/*
    @Override
    protected void reset1() {
        singleKey = null;
        singleValues = null;

        curRow = null;
    }
    */

}
