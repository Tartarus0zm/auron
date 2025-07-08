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

import java.io.ByteArrayOutputStream;
import java.util.LinkedList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.blaze.flink.arrow.ArrowUtils;
import org.blaze.flink.arrow.ArrowWriter;

public class RowData2ArrowOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private int count;
    private RowType inputRowType;
    private transient ArrowWriter arrowWriter;
    private transient ByteArrayOutputStream baos;
    private transient ArrowStreamWriter arrowStreamWriter;
    private transient int currentBatchSize;
    private transient List<RowData> buffer;

    public RowData2ArrowOperator(int count, RowType inputRowType) {
        this.count = count;
        this.inputRowType = inputRowType;
    }

    @Override
    public void open() throws Exception {
        DataType defaultRowDataType = TypeConversions.fromLogicalToDataType(inputRowType);
        BufferAllocator allocator =
                ArrowUtils.getRootAllocator().newChildAllocator("RowData2ArrowOperator", 0, Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(inputRowType), allocator);
        baos = new ByteArrayOutputStream();
        arrowStreamWriter = new ArrowStreamWriter(root, null, baos);
        arrowStreamWriter.start();
        arrowWriter = ArrowUtils.createRowDataArrowWriter(root, inputRowType);
        currentBatchSize = 0;
        buffer = new LinkedList<>();
    }

    @Override
    public void processElement(StreamRecord<RowData> streamRecord) throws Exception {
        RowData rowData = streamRecord.getValue();
        //        if (currentBatchSize < count) {
        //            buffer.add(rowData);
        //            currentBatchSize++;
        //        } else {
        //            arrowWriter.write(rowData);
        //            arrowStreamWriter.writeBatch();
        //            currentBatchSize = 0;
        //            buffer.clear();
        //            output.collect(new StreamRecord<>(rowData));
        //        }
        System.out.println("RowData2ArrowOperator processElement: " + rowData);
        output.collect(new StreamRecord<>(rowData));
    }

    @Override
    public void close() throws Exception {
        //        super.close();
        //        arrowStreamWriter.end();
        //        baos.close();
        //        arrowStreamWriter.close();
        //        arrowWriter.close();
    }
}
