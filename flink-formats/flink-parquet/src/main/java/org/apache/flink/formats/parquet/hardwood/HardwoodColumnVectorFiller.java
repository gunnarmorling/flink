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
import org.apache.flink.table.data.columnar.vector.heap.HeapArrayVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapBooleanVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapByteVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapDoubleVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapFloatVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapIntVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapLongVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapMapVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapShortVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapTimestampVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.LogicalType;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.Validity;
import dev.hardwood.schema.ColumnSchema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

/**
 * Fills Flink's {@link WritableColumnVector} from Hardwood's {@link ColumnReader} batch output.
 *
 * <p>Supports an offset into the Hardwood batch, allowing Flink to consume smaller slices of
 * Hardwood's larger batches (default 262K records). Handles flat primitives as well as ARRAY, MAP
 * and ROW types built from Hardwood's layer-model column API ({@code getLayerOffsets(layer)},
 * {@code getLayerValidity(layer)}, {@code getLeafValidity()}).
 */
public final class HardwoodColumnVectorFiller {

    private static final long NANOS_PER_MILLISECOND = 1_000_000L;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long MICROS_PER_MILLISECOND = 1_000L;
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final int INT96_BYTES = 12;

    private HardwoodColumnVectorFiller() {}

    /**
     * Physical encoding of a leaf column, derived once from its {@link ColumnSchema} so the hot
     * fill path never re-inspects the schema. {@code timestampUnit} defaults to {@code MILLIS} for
     * columns that are not annotated as a logical timestamp (e.g. INT96, which carries its own
     * nanosecond encoding and is decoded separately).
     */
    public static final class LeafEncoding {

        private final PhysicalType physicalType;
        private final dev.hardwood.metadata.LogicalType.TimeUnit timestampUnit;

        private LeafEncoding(
                PhysicalType physicalType,
                dev.hardwood.metadata.LogicalType.TimeUnit timestampUnit) {
            this.physicalType = physicalType;
            this.timestampUnit = timestampUnit;
        }

        public static LeafEncoding of(ColumnSchema schema) {
            dev.hardwood.metadata.LogicalType.TimeUnit unit =
                    dev.hardwood.metadata.LogicalType.TimeUnit.MILLIS;
            dev.hardwood.metadata.LogicalType logicalType = schema.logicalType();
            if (logicalType instanceof dev.hardwood.metadata.LogicalType.TimestampType) {
                unit = ((dev.hardwood.metadata.LogicalType.TimestampType) logicalType).unit();
            }
            return new LeafEncoding(schema.type(), unit);
        }

        public PhysicalType physicalType() {
            return physicalType;
        }

        public dev.hardwood.metadata.LogicalType.TimeUnit timestampUnit() {
            return timestampUnit;
        }
    }

    // ==================== Flat primitives ====================

    /**
     * Fill a WritableColumnVector from a slice of a Hardwood flat ColumnReader's current batch.
     *
     * @param fieldType the Flink logical type of the column
     * @param encoding the leaf's precomputed physical encoding
     * @param columnReader the Hardwood ColumnReader with an active batch
     * @param vector the target WritableColumnVector to fill
     * @param count the number of records to copy
     * @param offset the starting position within the Hardwood batch
     * @param isUtcTimestamp whether timestamps should be treated as UTC
     */
    public static void fillPrimitiveVector(
            LogicalType fieldType,
            LeafEncoding encoding,
            ColumnReader columnReader,
            WritableColumnVector vector,
            int count,
            int offset,
            boolean isUtcTimestamp) {
        fillLeafSlice(
                fieldType,
                encoding,
                columnReader,
                vector,
                count,
                offset,
                columnReader.getLeafValidity(),
                isUtcTimestamp);
    }

    /**
     * Fills {@code count} values starting at leaf-value index {@code base} into {@code dst}
     * (indexed from 0), marking nulls from {@code nulls}. Shared by the flat-column path and by
     * array elements / map entries — for a flat column the record index and leaf-value index
     * coincide, so {@code base} is the record offset; for nested leaves it is the first contained
     * value.
     */
    private static void fillLeafSlice(
            LogicalType type,
            LeafEncoding encoding,
            ColumnReader leaf,
            WritableColumnVector dst,
            int count,
            int base,
            Validity nulls,
            boolean isUtcTimestamp) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                fillBooleanVector(leaf, (HeapBooleanVector) dst, count, base, nulls);
                break;
            case TINYINT:
                fillByteVector(leaf, (HeapByteVector) dst, count, base, nulls);
                break;
            case SMALLINT:
                fillShortVector(leaf, (HeapShortVector) dst, count, base, nulls);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fillIntVector(leaf, (HeapIntVector) dst, count, base, nulls);
                break;
            case BIGINT:
                fillLongVector(leaf, (HeapLongVector) dst, count, base, nulls);
                break;
            case FLOAT:
                fillFloatVector(leaf, (HeapFloatVector) dst, count, base, nulls);
                break;
            case DOUBLE:
                fillDoubleVector(leaf, (HeapDoubleVector) dst, count, base, nulls);
                break;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                fillBytesVector(leaf, (HeapBytesVector) dst, count, base, nulls);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                fillTimestampVector(
                        leaf,
                        (HeapTimestampVector) dst,
                        count,
                        base,
                        nulls,
                        isUtcTimestamp,
                        encoding);
                break;
            case DECIMAL:
                fillDecimalVector(leaf, dst, count, base, nulls, encoding);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Hardwood reader does not yet support type: " + type);
        }
    }

    private static void fillBooleanVector(
            ColumnReader reader, HeapBooleanVector vector, int count, int offset, Validity nulls) {
        boolean[] values = reader.getBooleans();
        System.arraycopy(values, offset, vector.vector, 0, count);
        applyNullsToDst(vector, nulls, count, offset);
    }

    private static void fillByteVector(
            ColumnReader reader, HeapByteVector vector, int count, int offset, Validity nulls) {
        int[] values = reader.getInts();
        for (int i = 0; i < count; i++) {
            vector.vector[i] = (byte) values[offset + i];
        }
        applyNullsToDst(vector, nulls, count, offset);
    }

    private static void fillShortVector(
            ColumnReader reader, HeapShortVector vector, int count, int offset, Validity nulls) {
        int[] values = reader.getInts();
        for (int i = 0; i < count; i++) {
            vector.vector[i] = (short) values[offset + i];
        }
        applyNullsToDst(vector, nulls, count, offset);
    }

    private static void fillIntVector(
            ColumnReader reader, HeapIntVector vector, int count, int offset, Validity nulls) {
        int[] values = reader.getInts();
        System.arraycopy(values, offset, vector.vector, 0, count);
        applyNullsToDst(vector, nulls, count, offset);
    }

    private static void fillLongVector(
            ColumnReader reader, HeapLongVector vector, int count, int offset, Validity nulls) {
        long[] values = reader.getLongs();
        System.arraycopy(values, offset, vector.vector, 0, count);
        applyNullsToDst(vector, nulls, count, offset);
    }

    private static void fillFloatVector(
            ColumnReader reader, HeapFloatVector vector, int count, int offset, Validity nulls) {
        float[] values = reader.getFloats();
        System.arraycopy(values, offset, vector.vector, 0, count);
        applyNullsToDst(vector, nulls, count, offset);
    }

    private static void fillDoubleVector(
            ColumnReader reader, HeapDoubleVector vector, int count, int offset, Validity nulls) {
        double[] values = reader.getDoubles();
        System.arraycopy(values, offset, vector.vector, 0, count);
        applyNullsToDst(vector, nulls, count, offset);
    }

    private static void fillBytesVector(
            ColumnReader reader, HeapBytesVector vector, int count, int offset, Validity nulls) {
        byte[] buffer = reader.getBinaryValues();
        int[] offsets = reader.getBinaryOffsets();
        for (int i = 0; i < count; i++) {
            int srcIdx = offset + i;
            if (nulls.isNull(srcIdx)) {
                vector.setNullAt(i);
            } else {
                int start = offsets[srcIdx];
                vector.appendBytes(i, buffer, start, offsets[srcIdx + 1] - start);
            }
        }
    }

    private static void fillTimestampVector(
            ColumnReader reader,
            HeapTimestampVector vector,
            int count,
            int offset,
            Validity nulls,
            boolean isUtcTimestamp,
            LeafEncoding encoding) {

        if (encoding.physicalType() == PhysicalType.INT96) {
            byte[] buffer = reader.getBinaryValues();
            int[] offsets = reader.getBinaryOffsets();
            for (int i = 0; i < count; i++) {
                int srcIdx = offset + i;
                if (nulls.isNull(srcIdx)) {
                    vector.setNullAt(i);
                } else {
                    vector.setTimestamp(
                            i, int96ToTimestamp(buffer, offsets[srcIdx], isUtcTimestamp));
                }
            }
            return;
        }

        long[] values = reader.getLongs();
        dev.hardwood.metadata.LogicalType.TimeUnit unit = encoding.timestampUnit();
        for (int i = 0; i < count; i++) {
            int srcIdx = offset + i;
            if (nulls.isNull(srcIdx)) {
                vector.setNullAt(i);
            } else {
                vector.setTimestamp(i, int64ToTimestamp(values[srcIdx], unit, isUtcTimestamp));
            }
        }
    }

    /** Converts an INT64 timestamp at the given resolution to a {@link TimestampData}. */
    private static TimestampData int64ToTimestamp(
            long value, dev.hardwood.metadata.LogicalType.TimeUnit unit, boolean isUtcTimestamp) {
        switch (unit) {
            case MILLIS:
                return isUtcTimestamp
                        ? TimestampData.fromEpochMillis(value)
                        : TimestampData.fromTimestamp(new Timestamp(value));
            case MICROS:
                return scaledToTimestamp(
                        value, MICROS_PER_MILLISECOND, MICROS_PER_SECOND, isUtcTimestamp);
            case NANOS:
                return scaledToTimestamp(
                        value, NANOS_PER_MILLISECOND, NANOS_PER_SECOND, isUtcTimestamp);
            default:
                throw new UnsupportedOperationException("Unsupported timestamp unit: " + unit);
        }
    }

    /**
     * Converts a sub-millisecond epoch {@code value} (micros or nanos) to a {@link TimestampData}.
     * {@code perMilli} / {@code perSecond} are the number of sub-units per millisecond / second.
     * Uses floor semantics so pre-epoch (negative) values keep a non-negative sub-second remainder.
     */
    private static TimestampData scaledToTimestamp(
            long value, long perMilli, long perSecond, boolean isUtcTimestamp) {
        long millis = Math.floorDiv(value, perMilli);
        int nanoOfMillis =
                (int) (Math.floorMod(value, perMilli) * (NANOS_PER_MILLISECOND / perMilli));
        if (isUtcTimestamp) {
            return TimestampData.fromEpochMillis(millis, nanoOfMillis);
        }
        Timestamp ts = new Timestamp(millis);
        ts.setNanos((int) (Math.floorMod(value, perSecond) * (NANOS_PER_SECOND / perSecond)));
        return TimestampData.fromTimestamp(ts);
    }

    private static TimestampData int96ToTimestamp(byte[] bytes, int pos, boolean isUtcTimestamp) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, pos, INT96_BYTES).order(ByteOrder.LITTLE_ENDIAN);
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
            Validity nulls,
            LeafEncoding encoding) {

        switch (encoding.physicalType()) {
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
                        "Unsupported physical type for DECIMAL: " + encoding.physicalType());
        }
    }

    /**
     * Apply null flags from a Hardwood {@link Validity} indexed over the source batch to a Flink
     * vector indexed from 0.
     */
    private static void applyNullsToDst(
            WritableColumnVector vector, Validity nulls, int count, int offset) {
        if (!nulls.hasNulls()) {
            return;
        }
        int end = offset + count;
        for (int i = nulls.nextNull(offset, end); i >= 0; i = nulls.nextNull(i + 1, end)) {
            vector.setNullAt(i - offset);
        }
    }

    // ==================== ARRAY / MAP ====================

    /**
     * Fill a {@link HeapArrayVector} from a Hardwood nested leaf. The leaf must have nesting depth
     * 1 (single-level list). Nulls at the array level and at the element level are both honored.
     *
     * <p>{@code recordOffsets} and {@code recordValidity} are the leaf's REPEATED-layer offsets and
     * validity for the current batch; the caller supplies them so they are read once per batch and
     * reused for child sizing.
     */
    public static void fillArrayVector(
            LogicalType elementType,
            LeafEncoding elementEncoding,
            ColumnReader leafReader,
            int[] recordOffsets,
            Validity recordValidity,
            HeapArrayVector vector,
            int count,
            int offset,
            boolean isUtcTimestamp) {

        int firstValue = recordOffsets[offset];
        int valuesToCopy = recordOffsets[offset + count] - firstValue;

        WritableColumnVector childVec = childOf(vector);
        childVec.reset();
        if (valuesToCopy > 0) {
            fillLeafSlice(
                    elementType,
                    elementEncoding,
                    leafReader,
                    childVec,
                    valuesToCopy,
                    firstValue,
                    leafReader.getLeafValidity(),
                    isUtcTimestamp);
        }

        long[] outOffsets = new long[count];
        long[] outLengths = new long[count];
        for (int r = 0; r < count; r++) {
            int srcIdx = offset + r;
            int start = recordOffsets[srcIdx];
            outOffsets[r] = start - firstValue;
            outLengths[r] = recordOffsets[srcIdx + 1] - start;
            if (recordValidity.isNull(srcIdx)) {
                vector.setNullAt(r);
            }
        }
        vector.setOffsets(outOffsets);
        vector.setLengths(outLengths);
        vector.setSize(valuesToCopy);
    }

    /**
     * Fill a {@link HeapMapVector} from Hardwood key/value leaves. Both leaves share the same
     * REPEATED-layer offsets (guaranteed by the standard 3-level MAP encoding), supplied by the
     * caller as {@code recordOffsets} / {@code recordValidity}.
     */
    public static void fillMapVector(
            LogicalType keyType,
            LeafEncoding keyEncoding,
            LogicalType valueType,
            LeafEncoding valueEncoding,
            ColumnReader keyReader,
            ColumnReader valueReader,
            int[] recordOffsets,
            Validity recordValidity,
            HeapMapVector vector,
            int count,
            int offset,
            boolean isUtcTimestamp) {

        int firstValue = recordOffsets[offset];
        int valuesToCopy = recordOffsets[offset + count] - firstValue;

        WritableColumnVector keyVec = (WritableColumnVector) vector.getKeyColumnVector();
        WritableColumnVector valueVec = (WritableColumnVector) vector.getValueColumnVector();
        keyVec.reset();
        valueVec.reset();
        if (valuesToCopy > 0) {
            fillLeafSlice(
                    keyType,
                    keyEncoding,
                    keyReader,
                    keyVec,
                    valuesToCopy,
                    firstValue,
                    keyReader.getLeafValidity(),
                    isUtcTimestamp);
            fillLeafSlice(
                    valueType,
                    valueEncoding,
                    valueReader,
                    valueVec,
                    valuesToCopy,
                    firstValue,
                    valueReader.getLeafValidity(),
                    isUtcTimestamp);
        }

        long[] outOffsets = new long[count];
        long[] outLengths = new long[count];
        for (int r = 0; r < count; r++) {
            int srcIdx = offset + r;
            int start = recordOffsets[srcIdx];
            outOffsets[r] = start - firstValue;
            outLengths[r] = recordOffsets[srcIdx + 1] - start;
            if (recordValidity.isNull(srcIdx)) {
                vector.setNullAt(r);
            }
        }
        vector.setOffsets(outOffsets);
        vector.setLengths(outLengths);
        vector.setSize(valuesToCopy);
    }

    /**
     * Returns the writable child vector of a {@link HeapArrayVector}, unwrapping decimal if needed.
     */
    private static WritableColumnVector childOf(HeapArrayVector vector) {
        Object child = vector.getChild();
        if (child instanceof org.apache.flink.formats.parquet.vector.ParquetDecimalVector) {
            org.apache.flink.formats.parquet.vector.ParquetDecimalVector wrapper =
                    (org.apache.flink.formats.parquet.vector.ParquetDecimalVector) child;
            return (WritableColumnVector) wrapper.getVector();
        }
        return (WritableColumnVector) child;
    }
}
