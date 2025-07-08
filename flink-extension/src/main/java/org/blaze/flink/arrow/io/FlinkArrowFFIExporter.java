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
package org.blaze.flink.arrow.io;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.blaze.flink.arrow.ArrowUtils;
import org.blaze.flink.arrow.ArrowWriter;

/**
 * Arrow FFI exporter for flink passing data to blaze.
 */
public class FlinkArrowFFIExporter implements AutoCloseable {

    private VectorSchemaRoot currentRoot;
    private BlockingQueue<QueueState> outputQueue;
    private BlockingQueue<Void> processingQueue;
    private int maxBatchNumRows;
    private int maxBatchMemorySize;

    private DictionaryProvider.MapDictionaryProvider emptyDictionaryProvider;
    private Schema arrowSchema;
    private RowType inputType;

    private BufferAllocator allocator;
    private VectorSchemaRoot root;
    private ArrowWriter<RowData> arrowWriter;

    public FlinkArrowFFIExporter(RowType inputType, int maxBatchNumRows, int maxBatchNumBytes)
            throws InterruptedException {
        this.inputType = inputType;
        this.arrowSchema = ArrowUtils.toArrowSchema(inputType);
        this.maxBatchNumRows = maxBatchNumRows;
        this.maxBatchMemorySize = maxBatchNumBytes;
        this.emptyDictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
        this.outputQueue = new ArrayBlockingQueue<>(16);
        this.processingQueue = new ArrayBlockingQueue<>(16);
    }

    /**
     * open the exporter, called by flink operator
     */
    public void open() {
        allocator = ArrowUtils.getRootAllocator().newChildAllocator("FlinkArrowFFIExporter", 0, Long.MAX_VALUE);
        root = VectorSchemaRoot.create(arrowSchema, allocator);
        arrowWriter = ArrowUtils.createRowDataArrowWriter(root, inputType);
    }

    /**
     * called by blaze native code to export schema
     */
    public void exportSchema(Long exportArrowSchemaPtr) {
        Data.exportSchema(
                ArrowUtils.getRootAllocator(),
                arrowSchema,
                emptyDictionaryProvider,
                ArrowSchema.wrap(exportArrowSchemaPtr));
    }

    /**
     * called by blaze native code to export next batch
     */
    public boolean exportNextBatch(Long exportArrowArrayPtr) throws Throwable {
        if (!hasNext()) {
            return false;
        }

        // export using root allocator
        Data.exportVectorSchemaRoot(
                ArrowUtils.getRootAllocator(),
                currentRoot,
                emptyDictionaryProvider,
                ArrowArray.wrap(exportArrowArrayPtr));

        // to continue processing next batch
        processingQueue.put(Void.INSTANCE);
        return true;
    }

    private boolean hasNext() throws Throwable {
        QueueState state = outputQueue.take();
        if (state instanceof NextBatch) {
            return true;
        } else {
            if (((Finished) state).getThrowable() != null) {
                throw (((Finished) state).getThrowable());
            }
            return false;
        }
    }

    /**
     * called by flink operator to put data
     */
    public void putData(RowData input) throws Exception {
        if (allocator.getAllocatedMemory() < maxBatchMemorySize && arrowWriter.getCurrentRowCount() < maxBatchNumRows) {
            arrowWriter.write(input);
        } else {
            arrowWriter.finish();
            // export root
            currentRoot = root;
            outputQueue.put(NextBatch.INSTANCE);
            // wait for processing next batch
            processingQueue.take();
            // flink arrow writer support reset
            arrowWriter.reset();
        }
    }

    @Override
    public void close() throws Exception {
        outputQueue.put(new Finished());
    }
}
