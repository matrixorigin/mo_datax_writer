package com.alibaba.datax.plugin.writer.matrixonewriter.util;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;

public class RecordSerializerUtil {

    private final char columnSeparator;

    public RecordSerializerUtil(char sp) {
        this.columnSeparator = sp;
    }

    public String serialize(Record row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.getColumnNumber(); i++) {
            String value = fieldConvertion(row.getColumn(i));
            sb.append(null == value ? "\\N" : value);
            if (i < row.getColumnNumber() - 1) {
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }

    protected String fieldConvertion(Column col) {
        if (null == col.getRawData() || Column.Type.NULL == col.getType()) {
            return null;
        }
        if (Column.Type.BOOL == col.getType()) {
            return String.valueOf(col.asLong());
        }
        if (Column.Type.BYTES == col.getType()) {
            byte[] bts = (byte[])col.getRawData();
            long value = 0;
            for (int i = 0; i < bts.length; i++) {
                value += (bts[bts.length - i - 1] & 0xffL) << (8 * i);
            }
            return String.valueOf(value);
        }
        return col.asString();
    }

}
