package com.sl.connector.jdbc;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author L
 */
public class JdbcDataSource {

    private static final Logger logger = LoggerFactory.getLogger(JdbcDataSource.class);

    private final Map<String, BasicDataSource> dataSourceMap = new ConcurrentHashMap<>();

    public JdbcDataSource(JdbcConnectConfig config) {
        for (String jdbcUrl : config.getJdbcUrl()) {
            BasicDataSource dataSource = new BasicDataSource();
            dataSource.setDriverClassName(config.getDriverClass());
            dataSource.setUsername(config.getUser());
            dataSource.setPassword(config.getPassword());
            dataSource.setUrl(jdbcUrl);
            // 初始的连接数
            dataSource.setInitialSize(3);
            // 最大连接数
            dataSource.setMaxTotal(10);
            // 设置最大空闲连接
            dataSource.setMaxIdle(5);
            // 设置最大等待时间
            dataSource.setMaxWaitMillis(20000);
            // 设置最小空闲连接
            dataSource.setMinIdle(3);
            dataSource.setTestWhileIdle(true);
            dataSource.setConnectionProperties("keepAliveTimeout=10000;");
            dataSourceMap.put(jdbcUrl, dataSource);
            logger.info("jdbc连接, jdbcUrl: " + jdbcUrl);
        }
    }

    public void executeAllDs(String sql) throws SQLException {
        logger.info("执行sql: " + sql);
        for (Map.Entry<String, BasicDataSource> entry : dataSourceMap.entrySet()) {
            BasicDataSource dataSource = entry.getValue();
            Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.execute();
            connection.close();
        }
    }

    public void execute(String sql) throws SQLException {
        logger.info("执行sql: " + sql);
        BasicDataSource dataSource = dataSourceMap.values().iterator().next();
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.execute();
        connection.close();
    }

    public List<String> showDatabases() throws SQLException {
        BasicDataSource dataSource = dataSourceMap.values().iterator().next();
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement("show databases");
        ResultSet resultSet = statement.executeQuery();
        List<String> dbs = new ArrayList<>();
        while (resultSet.next()) {
            dbs.add(resultSet.getString(1));
        }
        connection.close();
        return dbs;
    }

    public List<String> showTables(String database) throws SQLException {
        BasicDataSource dataSource = dataSourceMap.values().iterator().next();
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format("show tables from `%s`", database));
        ResultSet resultSet = statement.executeQuery();
        List<String> dbs = new ArrayList<>();
        while (resultSet.next()) {
            dbs.add(resultSet.getString(1));
        }
        connection.close();
        return dbs;
    }

    public List<String> descTable(String table) throws SQLException {
        BasicDataSource dataSource = dataSourceMap.values().iterator().next();
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format("desc %s", table));
        ResultSet resultSet = statement.executeQuery();
        List<String> dbs = new ArrayList<>();
        while (resultSet.next()) {
            dbs.add(resultSet.getString(1));
        }
        connection.close();
        return dbs;
    }

    public Map<String, String> descTableColType(String table) throws SQLException {
        BasicDataSource dataSource = dataSourceMap.values().iterator().next();
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(String.format("desc %s", table));
        ResultSet resultSet = statement.executeQuery();
        Map<String, String> colTypes = new HashMap<>(16);
        while (resultSet.next()) {
            colTypes.put(resultSet.getString(1), resultSet.getString(2));
        }
        connection.close();
        return colTypes;
    }

    public boolean existAllDs(String database, String table) throws SQLException {
        for (Map.Entry<String, BasicDataSource> entry : dataSourceMap.entrySet()) {
            BasicDataSource dataSource = entry.getValue();
            Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(String.format("show tables from `%s`", database));
            ResultSet resultSet = statement.executeQuery();
            List<String> dbs = new ArrayList<>();
            while (resultSet.next()) {
                dbs.add(resultSet.getString(1));
            }
            if (!dbs.contains(table)) {
                logger.warn(String.format("jdbc 库: %s, 表: %s,不存在, 连接url: %s", database, table, dataSource.getUrl()));
                return false;
            }
            connection.close();
        }
        return true;
    }

    public List<Map<String, String>> executeQuery(String sql, String... columns) throws SQLException {
        logger.info("执行sql: " + sql);
        BasicDataSource dataSource = dataSourceMap.values().iterator().next();
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        List<Map<String, String>> valueList = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, String> values = new HashMap<>();
            for (String column : columns) {
                values.put(column, resultSet.getString(column));
            }
            valueList.add(values);
        }
        connection.close();
        return valueList;
    }

    public void close() throws SQLException {
        logger.info("关闭jdbc的连接");
        for (Map.Entry<String, BasicDataSource> entry : dataSourceMap.entrySet()) {
            BasicDataSource dataSource = entry.getValue();
            dataSource.close();
        }
    }

}
