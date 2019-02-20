package com.sl.connector.clickhouse;

import com.sl.connector.constant.ClickHouseTypeConstant;
import com.sl.connector.jdbc.JdbcConnectConfig;
import com.sl.connector.jdbc.JdbcDataSource;
import com.sl.connector.model.ConfigField;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.sl.connector.clickhouse.ClickHouseConfigField.*;
import static com.sl.connector.constant.CommonConstant.BOOLEAN_FALSE;
import static com.sl.connector.constant.CommonConstant.BOOLEAN_TRUE;
import static com.sl.connector.constant.CommonConstant.DISTRIBUTED;

/**
 * @author L
 */
public class ClickHouseConfigValidator {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseConfigValidator.class);

    /**
     * 校验参数非空和参数类型
     *
     * @param configMap      存储错误信息, 参数名为 key, ConfigValue 为value
     * @param configFieldMap 所有参数元数据
     * @param props          输入参数名和参数值
     */
    public void baseValidate(Map<String, ConfigValue> configMap, Map<String, ConfigField> configFieldMap, Map<String, String> props) {
        for (Map.Entry<String, ConfigField> entry : configFieldMap.entrySet()) {
            ConfigField field = entry.getValue();
            ConfigValue configValue = configMap.getOrDefault(field.getName(), new ConfigValue(field.getName()));
            String value = props.get(field.getName());
            if (field.isRequire() && StringUtils.isEmpty(value)) {
                configValue.addErrorMessage("参数: " + field.getName() + ", 不可为空");
            }
            if (StringUtils.isNotEmpty(value)) {
                try {
                    valueTypeValidate(field.getName(), value, field.getType());
                } catch (ConfigException e) {
                    configValue.addErrorMessage(e.getMessage());
                }
            }
        }
    }

    /**
     * 对 ClickHouse 参数校验
     * . ClickHouse 是否连接成功
     * . 输入数据库是否存在
     * . 输入表是否存在, 本地表与分布式表是否一致
     * . 输入字段是否存在, 是否是Date 类型
     *
     * @param configMap 存储错误信息, 参数名为 key, ConfigValue 为value
     * @param props     输入参数名和参数值
     */
    public void clickHouseValidate(Map<String, ConfigValue> configMap, Map<String, String> props) {
        // 连接是否成功
        ConfigValue hostConfig = configMap.get(CLICKHOUSE_HOSTS.getName());
        ConfigValue portConfig = configMap.get(CLICKHOUSE_JDBC_PORT.getName());
        ConfigValue userConfig = configMap.get(CLICKHOUSE_JDBC_USER.getName());
        ConfigValue passwordConfig = configMap.get(CLICKHOUSE_JDBC_PASSWORD.getName());
        ConfigValue databaseConfig = configMap.get(CLICKHOUSE_SINK_DATABASE.getName());
        if (hostConfig.errorMessages().isEmpty() && portConfig.errorMessages().isEmpty() && userConfig.errorMessages().isEmpty() &&
                passwordConfig.errorMessages().isEmpty() && databaseConfig.errorMessages().isEmpty()) {
            JdbcConnectConfig jdbcConfig = JdbcConnectConfig.initCkConnectConfig(props.get(CLICKHOUSE_HOSTS.getName()),
                    props.get(CLICKHOUSE_JDBC_PORT.getName()), props.get(CLICKHOUSE_JDBC_USER.getName()),
                    props.get(CLICKHOUSE_JDBC_PASSWORD.getName()));
            JdbcDataSource dataSource = new JdbcDataSource(jdbcConfig);
            String database = props.get(CLICKHOUSE_SINK_DATABASE.getName());
            try {
                databaseValidate(databaseConfig, dataSource, database);
                if (databaseConfig.errorMessages().isEmpty()) {
                    // 表校验
                    tableValidate(configMap, props, dataSource, database);
                    // 字段校验
                    sinkColumnValidate(configMap, props, dataSource, database);
                }
            } catch (Exception e) {
                logger.error("校验ClickHouse连接失败: ", e);
                hostConfig.addErrorMessage("连接ClickHouse失败, " + e.getMessage());
                try {
                    dataSource.close();
                } catch (SQLException e1) {
                    logger.error("关闭ClickHouse连接失败: ", e1);
                }
            }
        }
    }

    /**
     * 校验输入参数 clickhouse.source.date.format 是否是正确的日期格式化格式
     *
     * @param configMap 存储错误信息, 参数名为 key, ConfigValue 为value
     * @param props     输入参数名和参数值
     */
    public void dateformatValidate(Map<String, ConfigValue> configMap, Map<String, String> props) {
        ConfigValue dateformatConfig = configMap.get(CLICKHOUSE_SOURCE_DATE_FORMAT.getName());
        String dateformat = props.get(CLICKHOUSE_SOURCE_DATE_FORMAT.getName());
        if (StringUtils.isNotEmpty(dateformat)) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(dateformat);
                sdf.format(new Date());
            } catch (Exception e) {
                logger.info("校验输入时间格式: ", e);
                dateformatConfig.addErrorMessage("参数: " + dateformatConfig.name() + ", 值: " + dateformat + ", 不是正确的日期格式化格式");
            }
        }
    }

    /**
     * 校验输入字段是否存在, 是否是Date 类型
     */
    private void sinkColumnValidate(Map<String, ConfigValue> configMap, Map<String, String> props, JdbcDataSource dataSource, String database) throws SQLException {
        String table = props.get(CLICKHOUSE_SINK_TABLES.getName());
        Map<String, String> colTypeMap = dataSource.descTableColType(String.format("`%s`.`%s`", database, table));
        ConfigValue sinkDateColConfig = configMap.get(CLICKHOUSE_SINK_DATE_COLUMNS.getName());
        String sinkDateCol = props.get(CLICKHOUSE_SINK_DATE_COLUMNS.getName());
        String type = colTypeMap.get(sinkDateCol);
        if (StringUtils.isEmpty(type)) {
            sinkDateColConfig.addErrorMessage("参数: " + sinkDateColConfig.name() + ", 值: " + sinkDateCol + ", 字段不存在表中");
        } else if (!ClickHouseTypeConstant.DATE.equals(type)) {
            sinkDateColConfig.addErrorMessage("参数: " + sinkDateColConfig.name() + ", 值: " + sinkDateCol + ", 字段不是Date类型");
        }
    }

    /**
     * 校验输入表是否存在, 本地表与分布式表是否一致
     */
    private void tableValidate(Map<String, ConfigValue> configMap, Map<String, String> props, JdbcDataSource dataSource, String database) throws SQLException {
        ConfigValue sinkLocalTableConfig = configMap.get(CLICKHOUSE_SINK_LOCAL_TABLES.getName());
        ConfigValue sinkTableConfig = configMap.get(CLICKHOUSE_SINK_TABLES.getName());
        String localTable = props.get(CLICKHOUSE_SINK_LOCAL_TABLES.getName());
        String table = props.get(CLICKHOUSE_SINK_TABLES.getName());
        if (!dataSource.existAllDs(database, localTable)) {
            sinkLocalTableConfig.addErrorMessage("参数: " + sinkLocalTableConfig.name() + ", 值: " + localTable + ", 不存在集群中");
        }
        if (!dataSource.existAllDs(database, table)) {
            sinkTableConfig.addErrorMessage("参数: " + sinkTableConfig.name() + ", 值: " + table + ", 不存在集群中");
        }
        String sql = String.format("select engine,engine_full from system.tables where database='%s' and name='%s'", database, table);
        List<Map<String, String>> valueList = dataSource.executeQuery(sql, "engine", "engine_full");
        if (valueList == null || valueList.isEmpty()) {
            sinkTableConfig.addErrorMessage("参数: " + sinkTableConfig.name() + ", 值: " + table + ", 不存在集群中");
        } else {
            String engine = valueList.get(0).get("engine");
            String engineFull = valueList.get(0).get("engine_full");
            if (DISTRIBUTED.equalsIgnoreCase(engine)) {
                sinkTableConfig.addErrorMessage("参数: " + sinkTableConfig.name() + ", 值: " + table + ", 不是Distributed表");
            }
            List<String> engineFullList = Arrays.asList(engineFull.split(", "));
            if (!engineFullList.contains(localTable) && !engineFullList.contains(String.format("`%s`", localTable))) {
                sinkTableConfig.addErrorMessage("参数: " + sinkTableConfig.name() + ", 值: " + table + ", 其本地表不是表: " + localTable);
            }
        }
    }

    /**
     * 校验输入数据库是否存在
     */
    private void databaseValidate(ConfigValue databaseConfig, JdbcDataSource dataSource, String database) throws SQLException {
        List<String> databaseList = dataSource.showDatabases();
        if (!databaseList.contains(database)) {
            databaseConfig.addErrorMessage("参数: " + databaseConfig.name() + ", 值: " + database + ", 数据库不存在");
        }
    }

    /**
     * 输入参数的类型校验
     */
    private void valueTypeValidate(String name, String value, ConfigDef.Type type) {
        try {
            if (StringUtils.isEmpty(value)) {
                return;
            }
            switch (type) {
                case BOOLEAN:
                    if (BOOLEAN_TRUE.equalsIgnoreCase(value) || BOOLEAN_FALSE.equalsIgnoreCase(value)) {
                        throw new ConfigException("参数: " + name + ", 需为boolean类型");
                    }
                    break;
                case PASSWORD:
                    break;
                case STRING:
                    break;
                case INT:
                    Integer.parseInt(value);
                    break;
                case SHORT:
                    Short.parseShort(value);
                    break;
                case LONG:
                    Long.parseLong(value);
                    break;
                case DOUBLE:
                    Double.parseDouble(value);
                    break;
                case LIST:
                    break;
                case CLASS:
                    Class.forName(value, true, Utils.getContextOrKafkaClassLoader());
                    break;
                default:
                    throw new IllegalStateException("Unknown type.");
            }
        } catch (NumberFormatException e) {
            throw new ConfigException("参数: " + name + ", 需为数值类型");
        } catch (ClassNotFoundException e) {
            throw new ConfigException("参数: " + name + ", class不存在");
        }
    }

}
