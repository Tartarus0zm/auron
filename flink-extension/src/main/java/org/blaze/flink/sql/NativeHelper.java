/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.blaze.flink.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.blaze.protobuf.PhysicalPlanNode;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class NativeHelper {
    private Configuration conf = GlobalConfiguration.loadConfiguration();

    private long totalMemory = conf.get(TaskManagerOptions.TOTAL_PROCESS_MEMORY).getBytes();

    // TODO: nativeMemory = off-heap + over-head
    private long nativeMemory = conf.get(TaskManagerOptions.TASK_OFF_HEAP_MEMORY).getBytes();

    //TODO: flink plan
    public static boolean isNative(String plan) {
        return false;
    }

    public static Iterator<RowData> executeNativePlan(
            PhysicalPlanNode nativePlan,
            MetricGroup metrics) {

        if (nativePlan == null) {
            return Collections.emptyIterator();
        }
        return BlazeCallNativeWrapper(nativePlan, metrics).getRowIterator();
    }
}
