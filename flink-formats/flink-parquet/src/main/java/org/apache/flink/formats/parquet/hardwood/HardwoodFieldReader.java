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

import org.apache.flink.table.data.columnar.vector.heap.HeapArrayVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapBooleanVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapByteVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapDoubleVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapFloatVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapIntVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapLongVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapMapVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapRowVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapShortVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapTimestampVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.Validity;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads a single top-level Flink field from one or more Hardwood leaf {@link ColumnReader}s.
 *
 * <p>Each subclass encapsulates the mapping between a Flink {@link LogicalType} and the Parquet
 * leaf columns that back it:
 *
 * <ul>
 *   <li>{@link PrimitiveFieldReader} — one leaf column.
 *   <li>{@link ArrayFieldReader} — one leaf column, {@code maxRepetitionLevel == 1}.
 *   <li>{@link MapFieldReader} — two leaf columns (key and value).
 *   <li>{@link RowFieldReader} — one leaf column per struct field.
 *   <li>{@link MissingFieldReader} — no leaves; fills the vector with nulls.
 * </ul>
 */
public abstract class HardwoodFieldReader {

    protected final LogicalType type;

    protected HardwoodFieldReader(LogicalType type) {
        this.type = type;
    }

    /** Returns the leaf Hardwood column readers this field depends on. */
    public abstract ColumnReader[] leafReaders();

    /** Creates a fresh {@link WritableColumnVector} sized for one Flink batch. */
    public abstract WritableColumnVector createWritableVector(int batchSize);

    /**
     * Fills {@code vector} with {@code count} records starting at {@code offset} within the current
     * Hardwood batch.
     */
    public abstract void fillVector(
            WritableColumnVector vector, int count, int offset, boolean isUtcTimestamp);

    /** Fills the vector with nulls (used when the projected field is not present in the file). */
    public static HardwoodFieldReader missing(LogicalType type) {
        return new MissingFieldReader(type);
    }

    /**
     * Resolve a projected top-level field to a {@link HardwoodFieldReader}. Walks the Hardwood
     * schema tree to find the leaf columns and binds each to its coordinated {@link ColumnReader}
     * from {@code columnReaders}.
     */
    public static HardwoodFieldReader create(
            FileSchema fileSchema,
            ColumnReaders columnReaders,
            LogicalType flinkType,
            @Nullable String resolvedFieldName) {
        if (resolvedFieldName == null) {
            return missing(flinkType);
        }
        SchemaNode node = fileSchema.getField(resolvedFieldName);
        return create(fileSchema, columnReaders, flinkType, node, 0);
    }

    private static ColumnReader openLeaf(ColumnReaders columnReaders, ColumnSchema leaf) {
        return columnReaders.getColumnReader(leaf.fieldPath().toString());
    }

    /**
     * @param parentLayerCount the number of Hardwood layers (STRUCT/REPEATED) contributed by the
     *     groups enclosing {@code node}. This is the layer index at which {@code node}'s own
     *     REPEATED layer (ARRAY/MAP) or STRUCT layer (nullable ROW) lives within each leaf {@link
     *     ColumnReader}'s layer chain.
     */
    private static HardwoodFieldReader create(
            FileSchema fileSchema,
            ColumnReaders columnReaders,
            LogicalType flinkType,
            SchemaNode node,
            int parentLayerCount) {
        LogicalTypeRoot root = flinkType.getTypeRoot();
        switch (root) {
            case ARRAY:
                {
                    if (!(node instanceof SchemaNode.GroupNode)
                            || !((SchemaNode.GroupNode) node).isList()) {
                        throw new IllegalStateException(
                                "Expected LIST group for Flink ARRAY, found: " + node);
                    }
                    SchemaNode.GroupNode group = (SchemaNode.GroupNode) node;
                    SchemaNode element = group.getListElement();
                    if (!(element instanceof SchemaNode.PrimitiveNode)) {
                        throw new UnsupportedOperationException(
                                "Nested arrays are not yet supported; element is a group: "
                                        + element);
                    }
                    SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) element;
                    ColumnSchema leaf = fileSchema.getColumn(primitive.columnIndex());
                    ColumnReader leafReader = openLeaf(columnReaders, leaf);
                    LogicalType elementType = ((ArrayType) flinkType).getElementType();
                    return new ArrayFieldReader(
                            flinkType, elementType, leafReader, leaf, parentLayerCount);
                }
            case MAP:
            case MULTISET:
                {
                    if (!(node instanceof SchemaNode.GroupNode)
                            || !((SchemaNode.GroupNode) node).isMap()) {
                        throw new IllegalStateException(
                                "Expected MAP group for Flink " + root + ", found: " + node);
                    }
                    SchemaNode.GroupNode group = (SchemaNode.GroupNode) node;
                    SchemaNode keyValue = group.children().get(0);
                    if (!(keyValue instanceof SchemaNode.GroupNode)
                            || ((SchemaNode.GroupNode) keyValue).children().size() != 2) {
                        throw new IllegalStateException(
                                "Expected key_value group with two children, found: " + keyValue);
                    }
                    SchemaNode.GroupNode keyValueGroup = (SchemaNode.GroupNode) keyValue;
                    SchemaNode keyNode = keyValueGroup.children().get(0);
                    SchemaNode valueNode = keyValueGroup.children().get(1);
                    if (!(keyNode instanceof SchemaNode.PrimitiveNode)
                            || !(valueNode instanceof SchemaNode.PrimitiveNode)) {
                        throw new UnsupportedOperationException(
                                "Nested map key/value types are not yet supported: " + node);
                    }
                    SchemaNode.PrimitiveNode keyPrimitive = (SchemaNode.PrimitiveNode) keyNode;
                    SchemaNode.PrimitiveNode valuePrimitive = (SchemaNode.PrimitiveNode) valueNode;
                    ColumnSchema keyLeaf = fileSchema.getColumn(keyPrimitive.columnIndex());
                    ColumnSchema valueLeaf = fileSchema.getColumn(valuePrimitive.columnIndex());
                    ColumnReader keyReader = openLeaf(columnReaders, keyLeaf);
                    ColumnReader valueReader = openLeaf(columnReaders, valueLeaf);
                    LogicalType keyType;
                    LogicalType valueType;
                    if (root == LogicalTypeRoot.MAP) {
                        keyType = ((MapType) flinkType).getKeyType();
                        valueType = ((MapType) flinkType).getValueType();
                    } else {
                        keyType = ((MultisetType) flinkType).getElementType();
                        valueType = new org.apache.flink.table.types.logical.IntType(false);
                    }
                    return new MapFieldReader(
                            flinkType,
                            keyType,
                            valueType,
                            keyReader,
                            valueReader,
                            keyLeaf,
                            valueLeaf,
                            parentLayerCount);
                }
            case ROW:
                {
                    if (!(node instanceof SchemaNode.GroupNode)
                            || !((SchemaNode.GroupNode) node).isStruct()) {
                        throw new IllegalStateException(
                                "Expected struct group for Flink ROW, found: " + node);
                    }
                    SchemaNode.GroupNode group = (SchemaNode.GroupNode) node;
                    RowType rowType = (RowType) flinkType;
                    int fieldCount = rowType.getFieldCount();
                    if (group.children().size() != fieldCount) {
                        throw new IllegalStateException(
                                "ROW type field count mismatch: Flink="
                                        + fieldCount
                                        + ", Parquet="
                                        + group.children().size());
                    }
                    // An OPTIONAL struct group contributes a STRUCT layer carrying the row's own
                    // null bit; a REQUIRED struct contributes no layer and can never be null.
                    boolean nullable = group.repetitionType() == RepetitionType.OPTIONAL;
                    int structLayer = nullable ? parentLayerCount : -1;
                    int childLayerCount = parentLayerCount + (nullable ? 1 : 0);
                    List<HardwoodFieldReader> children = new ArrayList<>(fieldCount);
                    for (int i = 0; i < fieldCount; i++) {
                        SchemaNode child = group.children().get(i);
                        LogicalType childType = rowType.getTypeAt(i);
                        children.add(
                                create(
                                        fileSchema,
                                        columnReaders,
                                        childType,
                                        child,
                                        childLayerCount));
                    }
                    return new RowFieldReader(flinkType, children, structLayer);
                }
            default:
                {
                    if (!(node instanceof SchemaNode.PrimitiveNode)) {
                        throw new IllegalStateException(
                                "Expected primitive node for Flink " + root + ", found: " + node);
                    }
                    SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) node;
                    ColumnSchema leaf = fileSchema.getColumn(primitive.columnIndex());
                    ColumnReader leafReader = openLeaf(columnReaders, leaf);
                    return new PrimitiveFieldReader(flinkType, leafReader, leaf);
                }
        }
    }

    // ---------- Vector factories shared by all field-reader types ----------

    static WritableColumnVector createPrimitiveVector(
            int batchSize, LogicalType fieldType, @Nullable PhysicalType parquetPhysicalType) {
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return new HeapBooleanVector(batchSize);
            case TINYINT:
                return new HeapByteVector(batchSize);
            case SMALLINT:
                return new HeapShortVector(batchSize);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new HeapIntVector(batchSize);
            case BIGINT:
                return new HeapLongVector(batchSize);
            case FLOAT:
                return new HeapFloatVector(batchSize);
            case DOUBLE:
                return new HeapDoubleVector(batchSize);
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return new HeapBytesVector(batchSize);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new HeapTimestampVector(batchSize);
            case DECIMAL:
                return createDecimalVector(batchSize, (DecimalType) fieldType, parquetPhysicalType);
            default:
                throw new UnsupportedOperationException(
                        "Hardwood reader does not yet support type: " + fieldType);
        }
    }

    private static WritableColumnVector createDecimalVector(
            int batchSize, DecimalType decimalType, @Nullable PhysicalType parquetPhysicalType) {
        if (parquetPhysicalType != null) {
            switch (parquetPhysicalType) {
                case INT32:
                    return new HeapIntVector(batchSize);
                case INT64:
                    return new HeapLongVector(batchSize);
                default:
                    return new HeapBytesVector(batchSize);
            }
        }
        int precision = decimalType.getPrecision();
        if (org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.is32BitDecimal(
                precision)) {
            return new HeapIntVector(batchSize);
        } else if (org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.is64BitDecimal(
                precision)) {
            return new HeapLongVector(batchSize);
        } else {
            return new HeapBytesVector(batchSize);
        }
    }

    // ---------- Subclasses ----------

    /** Flat primitive field backed by a single Hardwood leaf. */
    private static final class PrimitiveFieldReader extends HardwoodFieldReader {

        private final ColumnReader leafReader;
        private final HardwoodColumnVectorFiller.LeafEncoding encoding;
        private final ColumnReader[] leafArray;

        PrimitiveFieldReader(LogicalType type, ColumnReader leafReader, ColumnSchema schema) {
            super(type);
            this.leafReader = leafReader;
            this.encoding = HardwoodColumnVectorFiller.LeafEncoding.of(schema);
            this.leafArray = new ColumnReader[] {leafReader};
        }

        @Override
        public ColumnReader[] leafReaders() {
            return leafArray;
        }

        @Override
        public WritableColumnVector createWritableVector(int batchSize) {
            return createPrimitiveVector(batchSize, type, encoding.physicalType());
        }

        @Override
        public void fillVector(
                WritableColumnVector vector, int count, int offset, boolean isUtcTimestamp) {
            HardwoodColumnVectorFiller.fillPrimitiveVector(
                    type, encoding, leafReader, vector, count, offset, isUtcTimestamp);
        }
    }

    /** Missing field: there is no column in the Parquet file, so fill with nulls. */
    private static final class MissingFieldReader extends HardwoodFieldReader {

        private MissingFieldReader(LogicalType type) {
            super(type);
        }

        @Override
        public ColumnReader[] leafReaders() {
            return new ColumnReader[0];
        }

        @Override
        public WritableColumnVector createWritableVector(int batchSize) {
            return createPrimitiveVector(batchSize, type, null);
        }

        @Override
        public void fillVector(
                WritableColumnVector vector, int count, int offset, boolean isUtcTimestamp) {
            vector.fillWithNulls();
        }
    }

    /** {@link ArrayType} field backed by one Hardwood nested leaf (nesting depth 1). */
    private static final class ArrayFieldReader extends HardwoodFieldReader {

        private final LogicalType elementType;
        private final ColumnReader leafReader;
        private final HardwoodColumnVectorFiller.LeafEncoding elementEncoding;
        private final ColumnReader[] leafArray;
        private final int repeatedLayer;

        ArrayFieldReader(
                LogicalType type,
                LogicalType elementType,
                ColumnReader leafReader,
                ColumnSchema leafSchema,
                int repeatedLayer) {
            super(type);
            this.elementType = elementType;
            this.leafReader = leafReader;
            this.elementEncoding = HardwoodColumnVectorFiller.LeafEncoding.of(leafSchema);
            this.leafArray = new ColumnReader[] {leafReader};
            this.repeatedLayer = repeatedLayer;
        }

        @Override
        public ColumnReader[] leafReaders() {
            return leafArray;
        }

        @Override
        public WritableColumnVector createWritableVector(int batchSize) {
            // Initial child has placeholder size; it is replaced per-batch in fillVector with one
            // sized to the actual element count for that batch.
            return new HeapArrayVector(batchSize, makeChild(batchSize));
        }

        private org.apache.flink.table.data.columnar.vector.ColumnVector makeChild(int size) {
            int safeSize = Math.max(size, 1);
            WritableColumnVector child =
                    createPrimitiveVector(safeSize, elementType, elementEncoding.physicalType());
            if (elementType.getTypeRoot() == LogicalTypeRoot.DECIMAL) {
                return new org.apache.flink.formats.parquet.vector.ParquetDecimalVector(child);
            }
            return child;
        }

        @Override
        public void fillVector(
                WritableColumnVector vector, int count, int offset, boolean isUtcTimestamp) {
            HeapArrayVector arrayVec = (HeapArrayVector) vector;
            int[] recordOffsets = leafReader.getLayerOffsets(repeatedLayer);
            Validity recordValidity = leafReader.getLayerValidity(repeatedLayer);
            int valuesToCopy = recordOffsets[offset + count] - recordOffsets[offset];
            arrayVec.setChild(makeChild(valuesToCopy));
            HardwoodColumnVectorFiller.fillArrayVector(
                    elementType,
                    elementEncoding,
                    leafReader,
                    recordOffsets,
                    recordValidity,
                    arrayVec,
                    count,
                    offset,
                    isUtcTimestamp);
        }
    }

    /** {@link MapType} (or {@link MultisetType}) field backed by key/value leaves. */
    private static final class MapFieldReader extends HardwoodFieldReader {

        private final LogicalType keyType;
        private final LogicalType valueType;
        private final ColumnReader keyReader;
        private final ColumnReader valueReader;
        private final HardwoodColumnVectorFiller.LeafEncoding keyEncoding;
        private final HardwoodColumnVectorFiller.LeafEncoding valueEncoding;
        private final ColumnReader[] leafArray;
        private final int repeatedLayer;

        MapFieldReader(
                LogicalType type,
                LogicalType keyType,
                LogicalType valueType,
                ColumnReader keyReader,
                ColumnReader valueReader,
                ColumnSchema keySchema,
                ColumnSchema valueSchema,
                int repeatedLayer) {
            super(type);
            this.keyType = keyType;
            this.valueType = valueType;
            this.keyReader = keyReader;
            this.valueReader = valueReader;
            this.keyEncoding = HardwoodColumnVectorFiller.LeafEncoding.of(keySchema);
            this.valueEncoding = HardwoodColumnVectorFiller.LeafEncoding.of(valueSchema);
            this.leafArray = new ColumnReader[] {keyReader, valueReader};
            this.repeatedLayer = repeatedLayer;
        }

        @Override
        public ColumnReader[] leafReaders() {
            return leafArray;
        }

        @Override
        public WritableColumnVector createWritableVector(int batchSize) {
            return new HeapMapVector(
                    batchSize, makeChild(batchSize, true), makeChild(batchSize, false));
        }

        private WritableColumnVector makeChild(int size, boolean key) {
            int safeSize = Math.max(size, 1);
            LogicalType type = key ? keyType : valueType;
            PhysicalType physical = key ? keyEncoding.physicalType() : valueEncoding.physicalType();
            return createPrimitiveVector(safeSize, type, physical);
        }

        @Override
        public void fillVector(
                WritableColumnVector vector, int count, int offset, boolean isUtcTimestamp) {
            HeapMapVector mapVec = (HeapMapVector) vector;
            int[] recordOffsets = keyReader.getLayerOffsets(repeatedLayer);
            Validity recordValidity = keyReader.getLayerValidity(repeatedLayer);
            int valuesToCopy = recordOffsets[offset + count] - recordOffsets[offset];
            mapVec.setKeys(makeChild(valuesToCopy, true));
            mapVec.setValues(makeChild(valuesToCopy, false));
            HardwoodColumnVectorFiller.fillMapVector(
                    keyType,
                    keyEncoding,
                    valueType,
                    valueEncoding,
                    keyReader,
                    valueReader,
                    recordOffsets,
                    recordValidity,
                    mapVec,
                    count,
                    offset,
                    isUtcTimestamp);
        }
    }

    /** {@link RowType} field backed by one Hardwood leaf per struct field. */
    private static final class RowFieldReader extends HardwoodFieldReader {

        private final List<HardwoodFieldReader> children;
        private final ColumnReader[] leafArray;
        private final int structLayer;

        RowFieldReader(LogicalType type, List<HardwoodFieldReader> children, int structLayer) {
            super(type);
            this.children = children;
            this.structLayer = structLayer;
            List<ColumnReader> flat = new ArrayList<>();
            for (HardwoodFieldReader child : children) {
                for (ColumnReader leaf : child.leafReaders()) {
                    flat.add(leaf);
                }
            }
            this.leafArray = flat.toArray(new ColumnReader[0]);
        }

        @Override
        public ColumnReader[] leafReaders() {
            return leafArray;
        }

        @Override
        public WritableColumnVector createWritableVector(int batchSize) {
            WritableColumnVector[] fields = new WritableColumnVector[children.size()];
            for (int i = 0; i < children.size(); i++) {
                fields[i] = children.get(i).createWritableVector(batchSize);
            }
            return new HeapRowVector(batchSize, fields);
        }

        @Override
        public void fillVector(
                WritableColumnVector vector, int count, int offset, boolean isUtcTimestamp) {
            HeapRowVector row = (HeapRowVector) vector;
            WritableColumnVector[] fields = row.getFields();
            for (int i = 0; i < children.size(); i++) {
                fields[i].reset();
                children.get(i).fillVector(fields[i], count, offset, isUtcTimestamp);
            }
            // A nullable (OPTIONAL) struct exposes its own null bit on the STRUCT layer that every
            // leaf beneath it shares, so read it from any leaf. A REQUIRED struct (structLayer < 0)
            // is never null. This replaces the former "all child fields null" heuristic, which
            // wrongly nulled a present struct whose fields happened to all be null.
            if (structLayer >= 0 && leafArray.length > 0) {
                Validity structValidity = leafArray[0].getLayerValidity(structLayer);
                if (structValidity.hasNulls()) {
                    for (int r = 0; r < count; r++) {
                        if (structValidity.isNull(offset + r)) {
                            row.setNullAt(r);
                        }
                    }
                }
            }
        }
    }
}
