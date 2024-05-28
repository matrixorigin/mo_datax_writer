package com.alibaba.datax.plugin.writer.matrixonewriter;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class MatrixoneWriterOptions implements Serializable {

    private static final long serialVersionUID = 1l;
    private static final long KILO_BYTES_SCALE = 1024l;
    private static final long MEGA_BYTES_SCALE = KILO_BYTES_SCALE * KILO_BYTES_SCALE;
    private static final int MAX_RETRIES = 1;
    private static final int BATCH_ROWS = 500000;
    private static final long BATCH_BYTES = 5 * MEGA_BYTES_SCALE;
    private static final long FLUSH_INTERVAL = 300000;

    public static final String KEY_USERNAME = "username";
    public static final String KEY_PASSWORD = "password";
    public static final String KEY_DATABASE = "database";
    public static final String KEY_TABLE = "table";
    public static final String KEY_COLUMN = "column";
    public static final String KEY_PRE_SQL = "preSql";
    public static final String KEY_POST_SQL = "postSql";
    public static final String KEY_JDBC_URL = "jdbcUrl";
	public static final String KEY_MAX_BATCH_ROWS = "maxBatchRows";
    public static final String KEY_MAX_BATCH_SIZE = "maxBatchSize";
    public static final String KEY_FLUSH_INTERVAL = "flushInterval";
    public static final String KEY_FLUSH_QUEUE_LENGTH = "flushQueueLength";
    public static final String CONNECTION_JDBC_URL = "connection[0].jdbcUrl";
    public static final String CONNECTION_TABLE_NAME = "connection[0].table[0]";
    public static final String CONNECTION_DATABASE = "connection[0].database";

    private final Configuration options;
    private List<String> infoSchemaColumns;
    private List<String> userSetColumns;
    private boolean isWildcardColumn;

    public MatrixoneWriterOptions(Configuration options) {
        this.options = options;
        // database
        String database = this.options.getString(CONNECTION_DATABASE);
        if (StringUtils.isBlank(database)) {
            database = this.options.getString(KEY_DATABASE);
        }
        if (StringUtils.isNotBlank(database)) {
            this.options.set(KEY_DATABASE, database);
        }
        // jdbcUrl
        String jdbcUrl = this.options.getString(CONNECTION_JDBC_URL);
        if (StringUtils.isNotBlank(jdbcUrl)) {
            this.options.set(KEY_JDBC_URL, jdbcUrl);
        }
        // table
        String table = this.options.getString(CONNECTION_TABLE_NAME);
        if (StringUtils.isNotBlank(table)) {
            this.options.set(KEY_TABLE, table);
        }
        // column
        this.userSetColumns = options.getList(KEY_COLUMN, String.class).stream().map(str -> str.replace("`", "")).collect(Collectors.toList());
        if (1 == options.getList(KEY_COLUMN, String.class).size() && "*".trim().equals(options.getList(KEY_COLUMN, String.class).get(0))) {
            this.isWildcardColumn = true;
        }
    }

    public void doPretreatment() {
        validateRequired();
    }

    public String getJdbcUrl() {
        return options.getString(KEY_JDBC_URL);
    }

    public String getDatabase() {
        return options.getString(KEY_DATABASE);
    }

    public String getTable() {
        return options.getString(KEY_TABLE);
    }

    public String getUsername() {
        return options.getString(KEY_USERNAME);
    }

    public String getPassword() {
        return options.getString(KEY_PASSWORD);
    }

    public List<String> getColumns() {
        if (isWildcardColumn) {
            return this.infoSchemaColumns;
        }
        return this.userSetColumns;
    }

    public boolean isWildcardColumn() {
        return this.isWildcardColumn;
    }

    public void setInfoSchemaColumns(List<String> cols) {
        this.infoSchemaColumns = cols;
    }

    public List<String> getPreSqlList() {
        return options.getList(KEY_PRE_SQL, String.class);
    }

    public List<String> getPostSqlList() {
        return options.getList(KEY_POST_SQL, String.class);
    }

    public int getMaxRetries() {
        return MAX_RETRIES;
    }

    public int getBatchRows() {
        Integer rows = options.getInt(KEY_MAX_BATCH_ROWS);
        return null == rows ? BATCH_ROWS : rows;
    }

    public long getBatchSize() {
        Long size = options.getLong(KEY_MAX_BATCH_SIZE);
        return null == size ? BATCH_BYTES : size;
    }

    public long getFlushInterval() {
        Long interval = options.getLong(KEY_FLUSH_INTERVAL);
        return null == interval ? FLUSH_INTERVAL : interval;
    }
    
    public int getFlushQueueLength() {
        Integer len = options.getInt(KEY_FLUSH_QUEUE_LENGTH);
        return null == len ? 1 : len;
    }


    private void validateRequired() {
       final String[] requiredOptionKeys = new String[]{
            KEY_USERNAME,
            KEY_DATABASE,
            KEY_TABLE,
            KEY_COLUMN
        };
        for (String optionKey : requiredOptionKeys) {
            options.getNecessaryValue(optionKey, DBUtilErrorCode.REQUIRED_VALUE);
        }
    }
}
