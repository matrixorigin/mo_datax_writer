package com.alibaba.datax.plugin.writer.matrixonewriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.matrixonewriter.manager.MatrixOneWriterManager;
import com.alibaba.datax.plugin.writer.matrixonewriter.util.RecordSerializerUtil;
import com.alibaba.datax.plugin.writer.matrixonewriter.util.MatrixOneWriterUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;


//TODO writeProxy
public class MatrixoneWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.MatrixOne;

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private MatrixoneWriterOptions options;

        @Override
        public void preCheck(){
            this.init();
            this.options.doPretreatment();
            MatrixOneWriterUtil.checkPreSQL(this.options);
            MatrixOneWriterUtil.checkPostSQL(this.options);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.options = new MatrixoneWriterOptions(this.originalConfig);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            // 执行preSQL
            MatrixOneWriterUtil.executeSQL(this.options, this.options.getPreSqlList());
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(originalConfig);
            }
            return configurations;
        }

        @Override
        public void post() {
            // 执行postSQL
            MatrixOneWriterUtil.executeSQL(this.options, this.options.getPostSqlList());
        }

        @Override
        public void destroy() {

        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private MatrixoneWriterOptions options;
        private MatrixOneWriterManager matrixOneWriterManager;

        private RecordSerializerUtil recordSerializerUtil;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.options = new MatrixoneWriterOptions(this.writerSliceConfig);
            if (options.isWildcardColumn()) {
                Connection conn = DBUtil.getConnection(DataBaseType.MatrixOne, options.getJdbcUrl(), options.getUsername(), options.getPassword());
                List<String> columns = MatrixOneWriterUtil.getTableColumns(conn, options.getDatabase(), options.getTable());
                options.setInfoSchemaColumns(columns);
            }
            this.matrixOneWriterManager = new MatrixOneWriterManager(this.options,DATABASE_TYPE);
            // 获取分隔符
            this.recordSerializerUtil = new RecordSerializerUtil(MatrixOneWriterManager.COLUMN_SEPARATOR);
        }

        @Override
        public void prepare() {

        }

        public void startWrite(RecordReceiver recordReceiver) {
            try {
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    if (record.getColumnNumber() != options.getColumns().size()) {
                        throw DataXException
                                .asDataXException(
                                        DBUtilErrorCode.CONF_ERROR,
                                        String.format(
                                                "Column configuration error. The number of reader columns %d and the number of writer columns %d are not equal.",
                                                record.getColumnNumber(),
                                                options.getColumns().size()));
                    }
                    String data = this.recordSerializerUtil.serialize(record);
                    this.matrixOneWriterManager.writeRecord(data);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
        }

        @Override
        public void post() {
            this.matrixOneWriterManager.close();
        }

        @Override
        public void destroy() {

        }

        @Override
        public boolean supportFailOver(){
            return false;
        }

    }


}
