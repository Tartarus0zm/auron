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
package org.blaze.flink.table.runtime.operator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.logical.RowType;
import org.blaze.flink.arrow.io.FlinkArrowFFIExporter;
import org.blaze.flink.config.FlinkBlazeOptions;
import org.blaze.flink.sql.JniBridge;
import org.blaze.flink.utils.NativeConverters;
import org.blaze.protobuf.*;

import java.util.Iterator;
import java.util.UUID;

public class FlinkBlazeCalcOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private RowType inputRowType;
    private transient FlinkArrowFFIExporter exporter;

    public FlinkBlazeCalcOperator(RowType inputRowType) {
        this.inputRowType = inputRowType;
    }

    @Override
    public void open() throws Exception {
        super.open();
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration();
        exporter = new FlinkArrowFFIExporter(
                inputRowType,
                flinkConfig.get(FlinkBlazeOptions.FLINK_BLAZE_ARROW_BATCH_SIZE),
                flinkConfig.get(FlinkBlazeOptions.FLINK_BLAZE_ARROW_MEM_SIZE));
        // init native helper
        //        NativeHelper.registerArrowFFI(exporter);
        // init blaze pb PhysicalPlanNode
        Schema nativeSchema = NativeConverters.convertToNativeSchema(inputRowType);
        String resourceId = "FlinkBlazeCalcOperator:" + UUID.randomUUID().toString();
        JniBridge.resourcesMap.put(resourceId, exporter);

    NativeHelper.executeNativePlan(
        PhysicalPlanNode.newBuilder()
            .setFfiReader(
                FFIReaderExecNode.newBuilder()
                    .setSchema(nativeSchema)
                    .setExportIterProviderResourceId(resourceId)
                    .build())
            .setProjection(ProjectionExecNode.newBuilder().addExpr(PhysicalExprNode.newBuilder().setBinaryExpr(
                    PhysicalBinaryExprNode.newBuilder().
            )))
            .build(),
        metrics);
    }

    @Override
    public void processElement(StreamRecord<RowData> streamRecord) throws Exception {
        RowData rowData = streamRecord.getValue();
        System.out.println("FlinkBlazeCalcOperator processElement: " + rowData);
        //data to blaze
        exporter.putData(rowData);
//        GenericRowData row = (GenericRowData) rowData;
//        row.setField(0, 100);
        Iterator iteratorRow = NativeHelper.executeNativePlan();
        while (iteratorRow.hasNext()) {
            output.collect(new StreamRecord<>(iteratorRow.next()));
        }
    }
}
