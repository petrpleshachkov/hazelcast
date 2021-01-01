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

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.sql.impl.calcite.opt.physical.FetchPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.FilterPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.SortPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ValuesPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;

/**
 * Visitor over physical relations.
 */
public interface PhysicalRelVisitor {
    void onRoot(RootPhysicalRel rel);
    void onMapScan(MapScanPhysicalRel rel);
    void onMapIndexScan(MapIndexScanPhysicalRel rel);
    void onRootExchange(RootExchangePhysicalRel rel);
    void onProject(ProjectPhysicalRel rel);
    void onFilter(FilterPhysicalRel rel);
    void onValues(ValuesPhysicalRel rel);
    void onSort(SortPhysicalRel rel);
    void onSortMergeExchange(SortMergeExchangePhysicalRel rel);
    void onFetch(FetchPhysicalRel rel);

}
