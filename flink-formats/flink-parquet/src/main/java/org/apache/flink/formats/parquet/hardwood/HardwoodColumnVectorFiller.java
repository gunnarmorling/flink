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

package org.apache.flink.formats.parquet.hardwood;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.heap.HeapBooleanVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapByteVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapDoubleVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapFloatVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapIntVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapLongVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapShortVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapTimestampVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.schema.ColumnSchema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.BitSet;
import java.util.concurrent.TimeUnit;

/**
 * Fills Flink's {@link WritableColumnVector} from Hardwood's {@link ColumnReader} batch output.
 *
 * <p>Supports an offset into the Hardwood batch, allowing Flink to consume smaller slices of
 * Hardwood's larger batches (default 262K records).
 */
public class HardwoodColumnVectorFiller {

    /**
     * Fill a WritableColumnVector from a slice of a Hardwood ColumnReader's current batch.
     *
     * @param fieldType the Flink logical type of the column
     * @param columnReader the Hardwood ColumnReader with an active batch
     * @param vector the target WritableColumnVector to fill
     * @param count the number of records to copy
     * @param offset the starting position within the Hardwood batch
     * @param isUtcTimestamp whether timestamps should be treated as UTC
     */
    public static void fillVector(
            LogicalType fieldType,
            ColumnReader columnReader,
            WritableColumnVector vector,
            int count,
            int offset,
            boolean isUtcTimestamp) {

        BitSet nulls = columnReader.getElementNulls();

        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                fillBooleanVector(columnReader, (HeapBooleanVector) vector, count, offset, nulls);
                break;
            case TINYINT:
                fillByteVector(columnReader, (HeapByteVector) vector, count, offset, nulls);
                break;
            case SMALLINT:
                fillShortVector(columnReader, (HeapShortVector) vector, count, offset, nulls);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fillIntVector(columnReader, (HeapIntVector) vector, count, offset, nulls);
                break;
            case BIGINT:
                fillLongVector(columnReader, (HeapLongVector) vector, count, offset, nulls);
                break;
            case FLOAT:
                fillFloatVector(columnReader, (HeapFloatVector) vector, count, offset, nulls);
                break;
            case DOUBLE:
                fillDoubleVector(columnReader, (HeapDoubleVector) vector, count, offset, nulls);
                break;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                fillBytesVector(columnReader, (HeapBytesVector) vector, count, offset, nulls);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fillTimestampVector(
                        columnReader,
                        (HeapTimestampVector) vector,
                        count,
                        offset,
                        nulls,
                        isUtcTimestamp);
                break;
            case DECIMAL:
                fillDecimalVector(
                        columnReader, vector, count, offset, nulls, (DecimalType) fieldType);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Hardwood reader does not yet support type: " + fieldType);
        }
    }

    private static void fillBooleanVector(
            ColumnReader reader, HeapBooleanVector vector, int count, int offset, BitSet nulls) {
        boolean[] values = reader.getBooleans();
        System.arraycopy(values, offset, vector.vector, 0, count);
        applyNulls(vector, nulls, count, offset);
    }

    private static void fillByteVector(
            ColumnReader reader, HeapByteVector vector, int count, int offset, BitSet nulls) {
        int[] values = reader.getInts();
        for (int i = 0; i < count; i++) {
            vector.vector[i] = (byte) values[offset + i];
        }
        applyNulls(vector, nulls, count, offset);
    }

    private static void fillShortVector(
            ColumnReader reader, HeapShortVector vector, int count, int offset, BitSet nulls) {
        int[] values = reader.getInts();
        for (int i = 0; i < count; i++) {
            vector.vector[i] = (short) values[offset + i];
        }
        applyNulls(vector, nulls, count, offset);
    }

    private static void fillIntVector(
            ColumnReader reader, HeapIntVector vector, int count, int offset, BitSet nulls) {
        int[] values = reader.getInts();
        System.arraycopy(values, offset, vector.vector, 0, count);
        applyNulls(vector, nulls, count, offset);
    }

    private static void fillLongVector(
            ColumnReader reader, HeapLongVector vector, int count, int offset, BitSet nulls) {
        long[] values = reader.getLongs();
        System.arraycopy(values, offset, vector.vector, 0, count);
        applyNulls(vector, nulls, count, offset);
    }

    private static void fillFloatVector(
            ColumnReader reader, HeapFloatVector vector, int count, int offset, BitSet nulls) {
        float[] values = reader.getFloats();
        System.arraycopy(values, offset, vector.vector, 0, count);
        applyNulls(vector, nulls, count, offset);
    }

    private static void fillDoubleVector(
            ColumnReader reader, HeapDoubleVector vector, int count, int offset, BitSet nulls) {
        double[] values = reader.getDoubles();
        System.arraycopy(values, offset, vector.vector, 0, count);
        applyNulls(vector, nulls, count, offset);
    }

    private static void fillBytesVector(
            ColumnReader reader, HeapBytesVector vector, int count, int offset, BitSet nulls) {
        byte[][] values = reader.getBinaries();
        for (int i = 0; i < count; i++) {
            int srcIdx = offset + i;
            if (nulls != null && nulls.get(srcIdx)) {
                vector.setNullAt(i);
            } else {
                byte[] bytes = values[srcIdx];
                vector.appendBytes(i, bytes, 0, bytes.length);
            }
        }
    }

    private static void fillTimestampVector(
            ColumnReader reader,
            HeapTimestampVector vector,
            int count,
            int offset,
            BitSet nulls,
            boolean isUtcTimestamp) {

        ColumnSchema schema = reader.getColumnSchema();
        dev.hardwood.metadata.PhysicalType physicalType = schema.type();

        if (physicalType == dev.hardwood.metadata.PhysicalType.INT96) {
            byte[][] values = reader.getBinaries();
            for (int i = 0; i < count; i++) {
                int srcIdx = offset + i;
                if (nulls != null && nulls.get(srcIdx)) {
                    vector.setNullAt(i);
                } else {
                    vector.setTimestamp(i, int96ToTimestamp(values[srcIdx], isUtcTimestamp));
                }
            }
        } else if (physicalType == dev.hardwood.metadata.PhysicalType.INT64) {
            long[] values = reader.getLongs();
            dev.hardwood.metadata.LogicalType logicalType = schema.logicalType();
            boolean isMicros = logicalType != null && logicalType.toString().contains("MICROS");
            for (int i = 0; i < count; i++) {
                int srcIdx = offset + i;
                if (nulls != null && nulls.get(srcIdx)) {
                    vector.setNullAt(i);
                } else {
                    if (isMicros) {
                        long micros = values[srcIdx];
                        long millis = micros / 1000;
                        int nanoAdj = (int) ((micros % 1000) * 1000);
                        if (isUtcTimestamp) {
                            if (nanoAdj < 0) {
                                millis--;
                                nanoAdj += 1_000_000;
                            }
                            vector.setTimestamp(i, TimestampData.fromEpochMillis(millis, nanoAdj));
                        } else {
                            Timestamp ts = new Timestamp(millis);
                            ts.setNanos((int) ((micros % 1_000_000) * 1000));
                            vector.setTimestamp(i, TimestampData.fromTimestamp(ts));
                        }
                    } else {
                        if (isUtcTimestamp) {
                            vector.setTimestamp(i, TimestampData.fromEpochMillis(values[srcIdx]));
                        } else {
                            Timestamp ts = new Timestamp(values[srcIdx]);
                            vector.setTimestamp(i, TimestampData.fromTimestamp(ts));
                        }
                    }
                }
            }
        }
    }

    private static final long NANOS_PER_MILLISECOND = 1_000_000L;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    private static TimestampData int96ToTimestamp(byte[] bytes, boolean isUtcTimestamp) {
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long nanos = buf.getLong();
        int julianDay = buf.getInt();
        long millis =
                (julianDay - 2440588L) * TimeUnit.DAYS.toMillis(1) + nanos / NANOS_PER_MILLISECOND;

        if (isUtcTimestamp) {
            int nanoOfMillis = (int) (nanos % NANOS_PER_MILLISECOND);
            return TimestampData.fromEpochMillis(millis, nanoOfMillis);
        } else {
            Timestamp timestamp = new Timestamp(millis);
            timestamp.setNanos((int) (nanos % NANOS_PER_SECOND));
            return TimestampData.fromTimestamp(timestamp);
        }
    }

    private static void fillDecimalVector(
            ColumnReader reader,
            WritableColumnVector vector,
            int count,
            int offset,
            BitSet nulls,
            DecimalType decimalType) {

        ColumnSchema schema = reader.getColumnSchema();
        dev.hardwood.metadata.PhysicalType physicalType = schema.type();

        switch (physicalType) {
            case INT32:
                fillIntVector(reader, (HeapIntVector) vector, count, offset, nulls);
                break;
            case INT64:
                fillLongVector(reader, (HeapLongVector) vector, count, offset, nulls);
                break;
            case BYTE_ARRAY:
            case FIXED_LEN_BYTE_ARRAY:
                fillBytesVector(reader, (HeapBytesVector) vector, count, offset, nulls);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported physical type for DECIMAL: " + physicalType);
        }
    }

    /**
     * Apply null flags from a Hardwood BitSet to a Flink vector.
     *
     * @param vector the target vector
     * @param nulls Hardwood's null bitmap (null means all non-null)
     * @param count number of records in the Flink batch
     * @param offset starting position in the Hardwood batch
     */
    private static void applyNulls(
            WritableColumnVector vector, BitSet nulls, int count, int offset) {
        if (nulls != null) {
            for (int i = nulls.nextSetBit(offset);
                    i >= 0 && i < offset + count;
                    i = nulls.nextSetBit(i + 1)) {
                vector.setNullAt(i - offset);
            }
        }
    }
}
