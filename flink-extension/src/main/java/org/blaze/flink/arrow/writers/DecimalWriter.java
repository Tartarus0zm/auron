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
package org.blaze.flink.arrow.writers;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.arrow.vector.DecimalVector;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;

/** {@link ArrowFieldWriter} for Decimal. */
@Internal
public abstract class DecimalWriter<T> extends ArrowFieldWriter<T> {

    public static DecimalWriter<RowData> forRow(DecimalVector decimalVector, int precision, int scale) {
        return new DecimalWriterForRow(decimalVector, precision, scale);
    }

    public static DecimalWriter<ArrayData> forArray(DecimalVector decimalVector, int precision, int scale) {
        return new DecimalWriterForArray(decimalVector, precision, scale);
    }

    // ------------------------------------------------------------------------------------------

    protected final int precision;
    protected final int scale;

    private DecimalWriter(DecimalVector decimalVector, int precision, int scale) {
        super(decimalVector);
        this.precision = precision;
        this.scale = scale;
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract DecimalData readDecimal(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((DecimalVector) getValueVector()).setNull(getCount());
        } else {
            BigDecimal bigDecimal = readDecimal(in, ordinal).toBigDecimal();
            bigDecimal = fromBigDecimal(bigDecimal, precision, scale);
            if (bigDecimal == null) {
                ((DecimalVector) getValueVector()).setNull(getCount());
            } else {
                ((DecimalVector) getValueVector()).setSafe(getCount(), bigDecimal);
            }
        }
    }

    /**
     * Convert the specified bigDecimal according to the specified precision and scale. The
     * specified bigDecimal may be rounded to have the specified scale and then the specified
     * precision is checked. If precision overflow, it will return `null`.
     *
     * <p>Note: The implementation refers to {@link DecimalData#fromBigDecimal}.
     */
    public static BigDecimal fromBigDecimal(BigDecimal bigDecimal, int precision, int scale) {
        if (bigDecimal.scale() != scale || bigDecimal.precision() > precision) {
            // need adjust the precision and scale
            bigDecimal = bigDecimal.setScale(scale, RoundingMode.HALF_UP);
            if (bigDecimal.precision() > precision) {
                return null;
            }
        }
        return bigDecimal;
    }

    // ------------------------------------------------------------------------------------------

    /** {@link DecimalWriter} for {@link RowData} input. */
    public static final class DecimalWriterForRow extends DecimalWriter<RowData> {

        private DecimalWriterForRow(DecimalVector decimalVector, int precision, int scale) {
            super(decimalVector, precision, scale);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        DecimalData readDecimal(RowData in, int ordinal) {
            return in.getDecimal(ordinal, precision, scale);
        }
    }

    /** {@link DecimalWriter} for {@link ArrayData} input. */
    public static final class DecimalWriterForArray extends DecimalWriter<ArrayData> {

        private DecimalWriterForArray(DecimalVector decimalVector, int precision, int scale) {
            super(decimalVector, precision, scale);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        DecimalData readDecimal(ArrayData in, int ordinal) {
            return in.getDecimal(ordinal, precision, scale);
        }
    }
}
