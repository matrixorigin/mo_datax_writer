package com.alibaba.datax.plugin.writer.matrixonewriter.manager;

import java.util.List;

public class MatrixOneFlushInfo {

    private Long bytes;
    private List<String> rows;

    public MatrixOneFlushInfo(Long bytes, List<String> rows) {
        this.bytes = bytes;
        this.rows = rows;
    }

    public Long getBytes() { return bytes; }
    public List<String> getRows() { return rows; }
}