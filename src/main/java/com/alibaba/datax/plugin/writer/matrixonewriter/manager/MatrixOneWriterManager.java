package com.alibaba.datax.plugin.writer.matrixonewriter.manager;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.matrixonewriter.MatrixoneWriterOptions;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

public class MatrixOneWriterManager {

    private static final Logger LOG = LoggerFactory.getLogger(MatrixOneWriterManager.class);

    private final MatrixoneWriterOptions writerOptions;

    private final List<String> rowBuffer = new ArrayList<>();
    private int batchCount = 0;
    private long batchSize = 0;
    private volatile boolean closed = false;
    private volatile Exception flushException;
    private final LinkedBlockingDeque<MatrixOneFlushInfo> flushQueue;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture;
    private final DataBaseType dataBaseType;

    public static final char COLUMN_SEPARATOR = 0X01;
    public static final String ROW_DELIMITER = "\r\n";

    public MatrixOneWriterManager(MatrixoneWriterOptions writerOptions, DataBaseType dataBaseType) {
        this.writerOptions = writerOptions;
        this.dataBaseType = dataBaseType;
        this.flushQueue = new LinkedBlockingDeque<>(writerOptions.getFlushQueueLength());
        this.startScheduler();
        this.startAsyncLoading();
    }

    public void startScheduler() {
        stopScheduler();
        this.scheduler = Executors.newScheduledThreadPool(1, new BasicThreadFactory.Builder().namingPattern("matrixone-interval-flush").daemon(true).build());
        this.scheduledFuture = this.scheduler.schedule(() -> {
            synchronized (MatrixOneWriterManager.this) {
                if (!closed) {
                    try {
                        loadData();
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }
        }, writerOptions.getFlushInterval(), TimeUnit.MILLISECONDS);
    }

    public void stopScheduler() {
        if (this.scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }
    }

    public final synchronized void writeRecord(String record) throws IOException {
        checkFlushException();
        try {
            rowBuffer.add(record);
            batchCount++;
            batchSize += record.getBytes(StandardCharsets.UTF_8).length;
            if (batchCount >= writerOptions.getBatchRows() || batchSize >= writerOptions.getBatchSize()) {
                loadData();
            }
        } catch (Exception e) {
            throw new IOException("loading records to MatrixOne failed. ", e);
        }
    }

    public synchronized void loadData() throws Exception {
        checkFlushException();
        if (batchCount == 0) {
            return;
        }
        LOG.debug("put data to queue batch rows {}, batch size {}", batchCount, batchSize);
        flushQueue.put(new MatrixOneFlushInfo(batchSize, new ArrayList<>(rowBuffer)));
        rowBuffer.clear();
        batchCount = 0;
        batchSize = 0;
    }

    public synchronized void close() {
        if (!closed) {
            closed = true;
            try {
                if (batchCount > 0) {
                    LOG.warn("load data to MatrixOne Sink is about to close");
                }
                loadData();
            } catch (Exception e) {
                throw new RuntimeException("Writing records to StarRocks failed.", e);
            }
        }
        checkFlushException();
    }

    private void startAsyncLoading() {
        // start flush thread
        Thread loadThread = new Thread(() -> {
            try {
                while (!closed || !flushQueue.isEmpty()) {
                    asyncLoadData();
                }
            } catch (Exception e) {
                flushException = e;
            }
        });
        //loadThread.setDaemon(true);
        loadThread.start();
    }

    private void asyncLoadData() throws Exception {
        MatrixOneFlushInfo flushData = flushQueue.take();
        if (Objects.isNull(flushData.getRows())) {
            return;
        }
        stopScheduler();
        for (int i = 0; i <= writerOptions.getMaxRetries(); i++) {
            try {
                Connection connection = DBUtil.getConnection(this.dataBaseType, this.writerOptions.getJdbcUrl(), this.writerOptions.getUsername(), this.writerOptions.getPassword());
                doLoadData(connection, flushData);
                startScheduler();
                break;
            } catch (Exception e) {
                LOG.warn("Failed to load batch data to MatrixOne, retry times = {}", i, e);
                if (i >= writerOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
            }
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to StarRocks failed.", flushException);
        }
    }

    protected void doLoadData(Connection connection, MatrixOneFlushInfo flushInfo)
            throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            connection.setAutoCommit(false);
            StringBuilder builder = new StringBuilder("load data inline format='csv', data=$XXX$");
            for (String row : flushInfo.getRows()) {
                builder.append(ROW_DELIMITER).append(row);
            }
            builder.append(" $XXX$").append("\n");
            builder.append("into table ").append(this.writerOptions.getTable()).append("\n");
            builder.append("fields terminated by ").append("'").append(COLUMN_SEPARATOR).append("'").append("\n");
            builder.append("lines terminated by ").append("'").append(StringEscapeUtils.escapeJava(ROW_DELIMITER)).append("'").append("\n");
            builder.append("(").append(String.join(",",this.writerOptions.getColumns())).append(")");
            preparedStatement = connection.prepareStatement(builder.toString());
            preparedStatement.execute();
            connection.commit();
        } catch (Exception e) {
            LOG.warn("load data to {} error: {}", this.writerOptions.getTable(), e.getMessage());
            connection.rollback();
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(preparedStatement, connection);
        }
    }

}
