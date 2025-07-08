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

package org.blaze.flink.utils;

import java.util.stream.Collectors;
import org.apache.flink.table.types.logical.*;
import org.blaze.protobuf.*;
import scala.NotImplementedError;

public class NativeConverters {

    public static Schema convertToNativeSchema(RowType rowType) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (RowType.RowField rowField : rowType.getFields()) {
            schemaBuilder.addColumns(convertField(rowField));
        }
        return schemaBuilder.build();
    }

    public static Field convertField(RowType.RowField rowField) {
        return Field.newBuilder()
                .setName(rowField.getName())
                .setNullable(rowField.getType().isNullable())
                .setArrowType(convertDataType(rowField.getType()))
                .build();
    }

    public static ArrowType convertDataType(LogicalType flinkLogicalType) {
        ArrowType.Builder arrowTypeBuilder = ArrowType.newBuilder();
        switch (flinkLogicalType.getTypeRoot()) {
            case NULL:
                arrowTypeBuilder.setNONE(EmptyMessage.getDefaultInstance());
                break;
            case BOOLEAN:
                arrowTypeBuilder.setBOOL(EmptyMessage.getDefaultInstance());
                break;
            case TINYINT:
                // case ByteType :
                arrowTypeBuilder.setINT8(EmptyMessage.getDefaultInstance());
                break;
            case SMALLINT:
                arrowTypeBuilder.setINT16(EmptyMessage.getDefaultInstance());
                break;
            case INTEGER:
                arrowTypeBuilder.setINT32(EmptyMessage.getDefaultInstance());
                break;
            case BIGINT:
                arrowTypeBuilder.setINT64(EmptyMessage.getDefaultInstance());
                break;
            case FLOAT:
                arrowTypeBuilder.setFLOAT32(EmptyMessage.getDefaultInstance());
                break;
            case DOUBLE:
                arrowTypeBuilder.setFLOAT64(EmptyMessage.getDefaultInstance());
                break;
            case VARCHAR:
                arrowTypeBuilder.setUTF8(EmptyMessage.getDefaultInstance());
                break;
            case BINARY:
                arrowTypeBuilder.setBINARY(EmptyMessage.getDefaultInstance());
                break;
            case DATE:
                arrowTypeBuilder.setDATE32(EmptyMessage.getDefaultInstance());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                // timezone is never used in native side
                arrowTypeBuilder.setTIMESTAMP(
                        org.blaze.protobuf.Timestamp.newBuilder().setTimeUnit(TimeUnit.Microsecond));
                break;
            case DECIMAL:
                // decimal
                DecimalType t = (DecimalType) flinkLogicalType;
                arrowTypeBuilder.setDECIMAL(org.blaze.protobuf.Decimal.newBuilder()
                        .setWhole(Math.max(t.getPrecision(), 1))
                        .setFractional(t.getScale())
                        .build());
                break;
            case ARRAY:
                // array/list
                ArrayType a = (ArrayType) flinkLogicalType;
                arrowTypeBuilder.setLIST(org.blaze.protobuf.List.newBuilder()
                        .setFieldType(Field.newBuilder()
                                .setName("item")
                                .setArrowType(convertDataType(a.getElementType()))
                                .setNullable(a.isNullable()))
                        .build());
                break;
            case MAP:
                MapType m = (MapType) flinkLogicalType;
                arrowTypeBuilder.setMAP(org.blaze.protobuf.Map.newBuilder()
                        .setKeyType(Field.newBuilder()
                                .setName("key")
                                .setArrowType(convertDataType(m.getKeyType()))
                                .setNullable(false))
                        .setValueType(Field.newBuilder()
                                .setName("value")
                                .setArrowType(convertDataType(m.getValueType()))
                                .setNullable(m.getValueType().isNullable()))
                        // .setNullable(m.isNullable()))
                        .build());
                break;
            case ROW:
                // StructType
                RowType r = (RowType) flinkLogicalType;
                arrowTypeBuilder.setSTRUCT(org.blaze.protobuf.Struct.newBuilder()
                        .addAllSubFieldTypes(r.getFields().stream()
                                .map(e -> Field.newBuilder()
                                        .setArrowType(convertDataType(e.getType()))
                                        .setName(e.getName())
                                        .setNullable(e.getType().isNullable())
                                        .build())
                                .collect(Collectors.toList()))
                        .build());
                break;
            default:
                throw new NotImplementedError(
                        "Data type conversion not implemented " + flinkLogicalType.asSummaryString());
        }
        return arrowTypeBuilder.build();
    }
}
