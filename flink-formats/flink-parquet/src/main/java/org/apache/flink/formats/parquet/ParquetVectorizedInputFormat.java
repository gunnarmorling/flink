/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.hardwood.HardwoodColumnVectorFiller;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.formats.parquet.vector.ColumnBatchFactory;
import org.apache.flink.formats.parquet.vector.ParquetDecimalVector;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Parquet {@link BulkFormat} that reads data from the file to {@link VectorizedColumnBatch} in
 * vectorized mode.
 */
public abstract class ParquetVectorizedInputFormat<T, SplitT extends FileSourceSplit>
        implements BulkFormat<T, SplitT> {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetVectorizedInputFormat.class);
    private static final long serialVersionUID = 1L;

    protected final SerializableConfiguration hadoopConfig;
    private final String[] projectedFields;
    private final LogicalType[] projectedTypes;
    private final ColumnBatchFactory<SplitT> batchFactory;
    private final int batchSize;
    protected final boolean isUtcTimestamp;
    private final boolean isCaseSensitive;

    public ParquetVectorizedInputFormat(
            SerializableConfiguration hadoopConfig,
            RowType projectedType,
            ColumnBatchFactory<SplitT> batchFactory,
            int batchSize,
            boolean isUtcTimestamp,
            boolean isCaseSensitive) {
        this.hadoopConfig = hadoopConfig;
        this.projectedFields = projectedType.getFieldNames().toArray(new String[0]);
        this.projectedTypes = projectedType.getChildren().toArray(new LogicalType[0]);
        this.batchFactory = batchFactory;
        this.batchSize = batchSize;
        this.isUtcTimestamp = isUtcTimestamp;
        this.isCaseSensitive = isCaseSensitive;
    }

    @Override
    public HardwoodReader createReader(final Configuration config, final SplitT split)
            throws IOException {

        final Path filePath = split.path();

        // Convert Flink path to java.nio.file.Path for Hardwood.
        // Flink paths may or may not have a scheme; normalize to file:// for local paths.
        java.net.URI uri = filePath.toUri();
        if (uri.getScheme() == null) {
            uri = new java.io.File(uri.getPath()).toURI();
        }
        java.nio.file.Path nioPath = java.nio.file.Path.of(uri);

        dev.hardwood.reader.ParquetFileReader hardwoodReader =
                dev.hardwood.reader.ParquetFileReader.open(nioPath);

        FileSchema fileSchema = hardwoodReader.getFileSchema();

        // Resolve projected column names in the file schema (case-sensitive or insensitive)
        String[] resolvedColumnNames = resolveColumnNames(fileSchema);

        // Count total rows across all row groups
        long totalRowCount = 0;
        for (dev.hardwood.metadata.RowGroup rg : hardwoodReader.getFileMetaData().rowGroups()) {
            totalRowCount += rg.numRows();
        }

        final Pool<ParquetReaderBatch<T>> poolOfBatches =
                createPoolOfBatches(
                        split, numBatchesToCirculate(config), fileSchema, resolvedColumnNames);

        return new HardwoodReader(
                hardwoodReader, resolvedColumnNames, totalRowCount, poolOfBatches);
    }

    protected int numBatchesToCirculate(Configuration config) {
        return config.get(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY);
    }

    @Override
    public HardwoodReader restoreReader(final Configuration config, final SplitT split)
            throws IOException {

        assert split.getReaderPosition().isPresent();
        final CheckpointedPosition checkpointedPosition = split.getReaderPosition().get();

        Preconditions.checkArgument(
                checkpointedPosition.getOffset() == CheckpointedPosition.NO_OFFSET,
                "The offset of CheckpointedPosition should always be NO_OFFSET");
        HardwoodReader reader = createReader(config, split);
        reader.seek(checkpointedPosition.getRecordsAfterOffset());
        return reader;
    }

    @Override
    public boolean isSplittable() {
        // Hardwood memory-maps entire files; split-level byte-range filtering is not supported.
        return false;
    }

    /**
     * Resolve projected field names against the Hardwood file schema, handling case-sensitive and
     * case-insensitive matching. Returns null for fields not found in the file (they will be filled
     * with nulls).
     */
    private String[] resolveColumnNames(FileSchema fileSchema) {
        String[] resolved = new String[projectedFields.length];

        if (isCaseSensitive) {
            for (int i = 0; i < projectedFields.length; i++) {
                try {
                    fileSchema.getField(projectedFields[i]);
                    resolved[i] = projectedFields[i];
                } catch (IllegalArgumentException e) {
                    LOG.warn(
                            "{} does not exist in file schema, will fill the field with null.",
                            projectedFields[i]);
                    resolved[i] = null;
                }
            }
        } else {
            // Build case-insensitive name map from top-level fields
            Map<String, String> caseInsensitiveMap = new HashMap<>();
            for (dev.hardwood.schema.SchemaNode child : fileSchema.getRootNode().children()) {
                caseInsensitiveMap.put(child.name().toLowerCase(Locale.ROOT), child.name());
            }
            for (int i = 0; i < projectedFields.length; i++) {
                String match = caseInsensitiveMap.get(projectedFields[i].toLowerCase(Locale.ROOT));
                if (match == null) {
                    LOG.warn(
                            "{} does not exist in file schema, will fill the field with null.",
                            projectedFields[i]);
                }
                resolved[i] = match;
            }
        }
        return resolved;
    }

    private Pool<ParquetReaderBatch<T>> createPoolOfBatches(
            SplitT split, int numBatches, FileSchema fileSchema, String[] resolvedColumnNames) {
        final Pool<ParquetReaderBatch<T>> pool = new Pool<>(numBatches);

        for (int i = 0; i < numBatches; i++) {
            pool.add(createReaderBatch(split, pool.recycler(), fileSchema, resolvedColumnNames));
        }

        return pool;
    }

    private ParquetReaderBatch<T> createReaderBatch(
            SplitT split,
            Pool.Recycler<ParquetReaderBatch<T>> recycler,
            FileSchema fileSchema,
            String[] resolvedColumnNames) {
        WritableColumnVector[] writableVectors =
                createWritableVectors(fileSchema, resolvedColumnNames);
        VectorizedColumnBatch columnarBatch =
                batchFactory.create(split, createReadableVectors(writableVectors));
        return createReaderBatch(writableVectors, columnarBatch, recycler);
    }

    private WritableColumnVector[] createWritableVectors(
            FileSchema fileSchema, String[] resolvedColumnNames) {
        WritableColumnVector[] columns = new WritableColumnVector[projectedTypes.length];
        for (int i = 0; i < projectedTypes.length; i++) {
            dev.hardwood.metadata.PhysicalType physicalType = null;
            if (resolvedColumnNames[i] != null
                    && projectedTypes[i].getTypeRoot() == LogicalTypeRoot.DECIMAL) {
                dev.hardwood.schema.SchemaNode node = fileSchema.getField(resolvedColumnNames[i]);
                if (node instanceof dev.hardwood.schema.SchemaNode.PrimitiveNode) {
                    physicalType = ((dev.hardwood.schema.SchemaNode.PrimitiveNode) node).type();
                }
            }
            columns[i] = createWritableVectorForType(batchSize, projectedTypes[i], physicalType);
        }
        return columns;
    }

    /**
     * Create a WritableColumnVector based on the Flink LogicalType. For DECIMAL columns, the actual
     * Parquet physical type is used to select the correct vector type, since the Parquet encoding
     * may differ from what Flink's precision-based heuristic would predict.
     */
    private static WritableColumnVector createWritableVectorForType(
            int batchSize,
            LogicalType fieldType,
            @Nullable dev.hardwood.metadata.PhysicalType parquetPhysicalType) {
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return new org.apache.flink.table.data.columnar.vector.heap.HeapBooleanVector(
                        batchSize);
            case TINYINT:
                return new org.apache.flink.table.data.columnar.vector.heap.HeapByteVector(
                        batchSize);
            case SMALLINT:
                return new org.apache.flink.table.data.columnar.vector.heap.HeapShortVector(
                        batchSize);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new org.apache.flink.table.data.columnar.vector.heap.HeapIntVector(
                        batchSize);
            case BIGINT:
                return new org.apache.flink.table.data.columnar.vector.heap.HeapLongVector(
                        batchSize);
            case FLOAT:
                return new org.apache.flink.table.data.columnar.vector.heap.HeapFloatVector(
                        batchSize);
            case DOUBLE:
                return new org.apache.flink.table.data.columnar.vector.heap.HeapDoubleVector(
                        batchSize);
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return new org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector(
                        batchSize);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new org.apache.flink.table.data.columnar.vector.heap.HeapTimestampVector(
                        batchSize);
            case DECIMAL:
                if (parquetPhysicalType != null) {
                    // Use actual Parquet physical type to match fillDecimalVector expectations
                    switch (parquetPhysicalType) {
                        case INT32:
                            return new org.apache.flink.table.data.columnar.vector.heap
                                    .HeapIntVector(batchSize);
                        case INT64:
                            return new org.apache.flink.table.data.columnar.vector.heap
                                    .HeapLongVector(batchSize);
                        default:
                            return new org.apache.flink.table.data.columnar.vector.heap
                                    .HeapBytesVector(batchSize);
                    }
                }
                // Fallback to precision-based heuristic when file schema is unavailable
                org.apache.flink.table.types.logical.DecimalType decimalType =
                        (org.apache.flink.table.types.logical.DecimalType) fieldType;
                int precision = decimalType.getPrecision();
                if (org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.is32BitDecimal(
                        precision)) {
                    return new org.apache.flink.table.data.columnar.vector.heap.HeapIntVector(
                            batchSize);
                } else if (org.apache.flink.formats.parquet.utils.ParquetSchemaConverter
                        .is64BitDecimal(precision)) {
                    return new org.apache.flink.table.data.columnar.vector.heap.HeapLongVector(
                            batchSize);
                } else {
                    return new org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector(
                            batchSize);
                }
            default:
                throw new UnsupportedOperationException(
                        "Hardwood reader does not yet support type: " + fieldType);
        }
    }

    /**
     * Create readable vectors from writable vectors. Especially for decimal, see {@link
     * ParquetDecimalVector}.
     */
    private ColumnVector[] createReadableVectors(WritableColumnVector[] writableVectors) {
        ColumnVector[] vectors = new ColumnVector[writableVectors.length];
        for (int i = 0; i < writableVectors.length; i++) {
            vectors[i] =
                    projectedTypes[i].getTypeRoot() == LogicalTypeRoot.DECIMAL
                            ? new ParquetDecimalVector(writableVectors[i])
                            : writableVectors[i];
        }
        return vectors;
    }

    /**
     * Inner reader that uses Hardwood's ColumnReader API for batch-oriented columnar reading.
     *
     * <p>Hardwood produces large batches (default 262K records). Flink consumes smaller batches
     * (typically 2048). This reader tracks position within a Hardwood batch and serves Flink-sized
     * slices without losing data.
     */
    private class HardwoodReader implements BulkFormat.Reader<T> {

        private dev.hardwood.reader.ParquetFileReader fileReader;

        /** One Hardwood ColumnReader per projected column. Null if column is missing. */
        private ColumnReader[] columnReaders;

        private final long totalRowCount;
        private final Pool<ParquetReaderBatch<T>> pool;

        private long rowsReturned;
        private long recordsToSkip;
        private boolean readersInitialized;

        /** Resolved column names in the file schema. Null entry means column is missing. */
        private final String[] resolvedColumnNames;

        /** Current position within the active Hardwood batch. */
        private int hardwoodBatchOffset;

        /** Number of records in the current Hardwood batch. */
        private int hardwoodBatchSize;

        /** Whether there is an active Hardwood batch with remaining data. */
        private boolean hasHardwoodBatch;

        private HardwoodReader(
                dev.hardwood.reader.ParquetFileReader fileReader,
                String[] resolvedColumnNames,
                long totalRowCount,
                Pool<ParquetReaderBatch<T>> pool) {
            this.fileReader = fileReader;
            this.resolvedColumnNames = resolvedColumnNames;
            this.totalRowCount = totalRowCount;
            this.pool = pool;
            this.rowsReturned = 0;
            this.recordsToSkip = 0;
            this.readersInitialized = false;
            this.hardwoodBatchOffset = 0;
            this.hardwoodBatchSize = 0;
            this.hasHardwoodBatch = false;
        }

        @Nullable
        @Override
        public RecordIterator<T> readBatch() throws IOException {
            final ParquetReaderBatch<T> batch = getCachedEntry();

            final long rowsReturnedBefore = rowsReturned;
            if (!nextBatch(batch)) {
                batch.recycle();
                return null;
            }

            final RecordIterator<T> records = batch.convertAndGetIterator(rowsReturnedBefore);
            skipRecord(records);
            return records;
        }

        private boolean nextBatch(ParquetReaderBatch<T> batch) throws IOException {
            for (WritableColumnVector v : batch.writableVectors) {
                v.reset();
            }
            batch.columnarBatch.setNumRows(0);

            if (rowsReturned >= totalRowCount) {
                return false;
            }

            if (!readersInitialized) {
                initColumnReaders();
            }

            // If we've consumed the current Hardwood batch, fetch the next one
            if (!hasHardwoodBatch || hardwoodBatchOffset >= hardwoodBatchSize) {
                if (!advanceHardwoodBatch()) {
                    return false;
                }
            }

            // Determine how many records to serve in this Flink batch
            int remaining = hardwoodBatchSize - hardwoodBatchOffset;
            int num = (int) Math.min(Math.min(remaining, batchSize), totalRowCount - rowsReturned);

            for (int i = 0; i < columnReaders.length; i++) {
                if (columnReaders[i] == null) {
                    batch.writableVectors[i].fillWithNulls();
                } else {
                    HardwoodColumnVectorFiller.fillVector(
                            projectedTypes[i],
                            columnReaders[i],
                            batch.writableVectors[i],
                            num,
                            hardwoodBatchOffset,
                            isUtcTimestamp);
                }
            }

            hardwoodBatchOffset += num;
            rowsReturned += num;
            batch.columnarBatch.setNumRows(num);
            return true;
        }

        /** Advance all column readers to the next Hardwood batch. Returns false if no more data. */
        private boolean advanceHardwoodBatch() {
            hardwoodBatchOffset = 0;
            hardwoodBatchSize = 0;
            hasHardwoodBatch = false;

            for (int i = 0; i < columnReaders.length; i++) {
                if (columnReaders[i] != null) {
                    if (!hasHardwoodBatch) {
                        if (!columnReaders[i].nextBatch()) {
                            return false;
                        }
                        hardwoodBatchSize = columnReaders[i].getRecordCount();
                        hasHardwoodBatch = true;
                    } else {
                        columnReaders[i].nextBatch();
                    }
                }
            }
            return hasHardwoodBatch;
        }

        private void initColumnReaders() {
            columnReaders = new ColumnReader[resolvedColumnNames.length];
            for (int i = 0; i < resolvedColumnNames.length; i++) {
                if (resolvedColumnNames[i] != null) {
                    columnReaders[i] = fileReader.createColumnReader(resolvedColumnNames[i]);
                }
            }
            readersInitialized = true;
        }

        public void seek(long rowCount) {
            // Hardwood reads entire files; skip records from the beginning.
            this.recordsToSkip = rowCount;
        }

        private ParquetReaderBatch<T> getCachedEntry() throws IOException {
            try {
                return pool.pollEntry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted");
            }
        }

        private void skipRecord(RecordIterator<T> records) {
            while (recordsToSkip > 0 && records.next() != null) {
                recordsToSkip--;
            }
        }

        @Override
        public void close() throws IOException {
            if (columnReaders != null) {
                for (ColumnReader cr : columnReaders) {
                    if (cr != null) {
                        cr.close();
                    }
                }
                columnReaders = null;
            }
            if (fileReader != null) {
                fileReader.close();
                fileReader = null;
            }
        }
    }

    // ----------------------- Abstract method and class --------------------------

    /**
     * @param writableVectors vectors to be write
     * @param columnarBatch vectors to be read
     * @param recycler batch recycler
     */
    protected abstract ParquetReaderBatch<T> createReaderBatch(
            WritableColumnVector[] writableVectors,
            VectorizedColumnBatch columnarBatch,
            Pool.Recycler<ParquetReaderBatch<T>> recycler);

    /**
     * Reader batch that provides writing and reading capabilities. Provides {@link RecordIterator}
     * reading interface from {@link #convertAndGetIterator(long)}.
     */
    protected abstract static class ParquetReaderBatch<T> {

        private final WritableColumnVector[] writableVectors;
        protected final VectorizedColumnBatch columnarBatch;
        private final Pool.Recycler<ParquetReaderBatch<T>> recycler;

        protected ParquetReaderBatch(
                WritableColumnVector[] writableVectors,
                VectorizedColumnBatch columnarBatch,
                Pool.Recycler<ParquetReaderBatch<T>> recycler) {
            this.writableVectors = writableVectors;
            this.columnarBatch = columnarBatch;
            this.recycler = recycler;
        }

        public void recycle() {
            recycler.recycle(this);
        }

        /**
         * Provides reading iterator after the records are written to the {@link #columnarBatch}.
         *
         * @param rowsReturned The number of rows that have been returned before this batch.
         */
        public abstract RecordIterator<T> convertAndGetIterator(long rowsReturned)
                throws IOException;
    }
}
