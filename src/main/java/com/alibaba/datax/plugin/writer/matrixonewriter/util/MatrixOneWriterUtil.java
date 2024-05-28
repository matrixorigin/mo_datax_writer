package com.alibaba.datax.plugin.writer.matrixonewriter.util;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.writer.matrixonewriter.MatrixoneWriterOptions;
import com.alibaba.druid.sql.parser.ParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author KJG-999
 * @date 2024/5/15 9:12
 * @description
 */
public class MatrixOneWriterUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MatrixOneWriterUtil.class);

    public static List<String> getTableColumns(Connection conn, String databaseName, String tableName) {
        String currentSql = String.format("SELECT attname FROM `mo_catalog`.`mo_columns` WHERE `att_database` = '%s' AND `att_relname` = '%s' AND `attname` <> '__mo_fake_pk_col' AND `attname` <> '__mo_rowid' ORDER BY `attnum` ASC;", databaseName, tableName);
        List<String> columns = new ArrayList<>();
        ResultSet rs = null;
        try {
            rs = DBUtil.query(conn, currentSql);
            while (DBUtil.asyncResultSetNext(rs)) {
                String colName = rs.getString("attname");
                columns.add(colName);
            }
            return columns;
        } catch (Exception e) {
            throw RdbmsException.asQueryException(DataBaseType.MatrixOne, e, currentSql, null, null);
        } finally {
            DBUtil.closeDBResources(rs, null, conn);
        }
    }

    public static void checkPreSQL(MatrixoneWriterOptions options) {
        List<String> preSqls = options.getPreSqlList();
        if (null != preSqls && !preSqls.isEmpty()) {
            LOG.info("Begin to preCheck preSqls:[{}].", String.join(";", preSqls));
            for (String sql : preSqls) {
                try {
                    DBUtil.sqlValid(sql, DataBaseType.MatrixOne);
                } catch (ParserException e) {
                    throw RdbmsException.asPreSQLParserException(DataBaseType.MatrixOne,e,sql);
                }
            }
        }
    }

    public static void checkPostSQL(MatrixoneWriterOptions options) {
        List<String> postSqls = options.getPostSqlList();
        if (null != postSqls && !postSqls.isEmpty()) {
            LOG.info("Begin to preCheck postSqls:[{}].", String.join(";", postSqls));
            for(String sql : postSqls) {
                try {
                    DBUtil.sqlValid(sql, DataBaseType.MatrixOne);
                } catch (ParserException e){
                    throw RdbmsException.asPostSQLParserException(DataBaseType.MatrixOne,e,sql);
                }
            }
        }
    }

    public static void executeSQL(MatrixoneWriterOptions options, List<String> executeSqls) {
        if (null != executeSqls && !executeSqls.isEmpty()) {
            Connection connection = null;
            try {
                connection = DBUtil.getConnection(DataBaseType.MatrixOne, options.getJdbcUrl(), options.getUsername(), options.getPassword());
                Statement stmt = null;
                String currentSql = null;
                try {
                    stmt = connection.createStatement();
                    for (String sql : executeSqls) {
                        currentSql = sql;
                        DBUtil.executeSqlWithoutResultSet(stmt, sql);
                    }
                } catch (Exception e) {
                    throw RdbmsException.asQueryException(DataBaseType.MatrixOne, e, currentSql, null, null);
                } finally {
                    DBUtil.closeDBResources(null, stmt, null);
                }
            } finally {
                DBUtil.closeDBResources(null, null, connection);
            }
        }
    }
}
