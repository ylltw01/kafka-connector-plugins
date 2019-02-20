package com.sl.connector.clickhouse;

import com.sl.connector.model.ConfigField;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.HashMap;
import java.util.Map;

/**
 * @author L
 */
public class ClickHouseConfigField {

    public static final ConfigField TOPIC = new ConfigField("topics", "消费的Kafka topic", Type.STRING,
            Importance.HIGH, true);

    public static final ConfigField CLICKHOUSE_HOSTS = new ConfigField("clickhouse.hosts", "连接clickhouse的host列表, 以逗号分隔, 不可为空",
            Type.STRING, Importance.HIGH, true);

    public static final ConfigField CLICKHOUSE_JDBC_PORT = new ConfigField("clickhouse.jdbc.port", "clickhouse jdbc连接端口号, 不可为空",
            Type.STRING, Importance.HIGH, true);

    public static final ConfigField CLICKHOUSE_JDBC_USER = new ConfigField("clickhouse.jdbc.user", "clickhouse jdbc连接用户名",
            "", Type.STRING, Importance.LOW, false);

    public static final ConfigField CLICKHOUSE_JDBC_PASSWORD = new ConfigField("clickhouse.jdbc.password", "clickhouse jdbc连接密码",
            "", Type.STRING, Importance.LOW, false);

    public static final ConfigField CLICKHOUSE_SINK_DATABASE = new ConfigField("clickhouse.sink.database", "目的clickhouse数据库, 不可为空",
            Type.STRING, Importance.HIGH, true);

    public static final ConfigField CLICKHOUSE_SINK_TABLES = new ConfigField("clickhouse.sink.tables", "目的clickhouse分布式表名, 不可为空",
            Type.STRING, Importance.HIGH, true);

    public static final ConfigField CLICKHOUSE_SINK_LOCAL_TABLES = new ConfigField("clickhouse.sink.local.tables",
            "目的clickhouse本地表名, 需是分布式表所依赖的本地表, 当clickhouse.optimize参数为true时不可为空",
            Type.STRING, Importance.LOW, false);

    public static final ConfigField CLICKHOUSE_SINK_DATE_COLUMNS = new ConfigField("clickhouse.sink.date.columns",
            "目的clickhouse表中的日期分区字段名, 不可为空",
            Type.STRING, Importance.HIGH, true);

    public static final ConfigField CLICKHOUSE_SOURCE_DATE_COLUMNS = new ConfigField("clickhouse.source.date.columns",
            "目的clickhouse表中的时间分区字段所取值的源字段名." +
                    " 1.当此参数为空时, 将默认取当前处理的时间写至clickhouse.sink.date.columns所描述的字段中" +
                    " 2.当此参数不为空时, 该参数值应存在在topics参数的字段值",
            Type.STRING, Importance.LOW, false);

    public static final ConfigField CLICKHOUSE_SOURCE_DATE_FORMAT = new ConfigField("clickhouse.source.date.format",
            "clickhouse.source.date.columns所描述字段的时间格式" +
                    " 1.当此参数为空时, 将默认yyyy-MM-dd HH:mm:ss格式化" +
                    " 2.当此参数不为空时, 将使用该参数的格式去格式化clickhouse.source.date.columns所描述字段的值",
            "yyyy-MM-dd HH:mm:ss", Type.STRING, Importance.LOW, false);

    public static final ConfigField CLICKHOUSE_OPTIMIZE = new ConfigField("clickhouse.optimize",
            "目的clickhouse本地表, 是否需要执行optimize, 本地表需是MergeTree系列引擎",
            false, Type.BOOLEAN, Importance.LOW, false);

    public static final Map<String, ConfigField> CLICKHOUSE_CONFIG_MAP = new HashMap<String, ConfigField>() {{
        put(TOPIC.getName(), TOPIC);
        put(CLICKHOUSE_HOSTS.getName(), CLICKHOUSE_HOSTS);
        put(CLICKHOUSE_JDBC_PORT.getName(), CLICKHOUSE_JDBC_PORT);
        put(CLICKHOUSE_JDBC_USER.getName(), CLICKHOUSE_JDBC_USER);
        put(CLICKHOUSE_JDBC_PASSWORD.getName(), CLICKHOUSE_JDBC_PASSWORD);
        put(CLICKHOUSE_SINK_DATABASE.getName(), CLICKHOUSE_SINK_DATABASE);
        put(CLICKHOUSE_SINK_TABLES.getName(), CLICKHOUSE_SINK_TABLES);
        put(CLICKHOUSE_SINK_LOCAL_TABLES.getName(), CLICKHOUSE_SINK_LOCAL_TABLES);
        put(CLICKHOUSE_SINK_DATE_COLUMNS.getName(), CLICKHOUSE_SINK_DATE_COLUMNS);
        put(CLICKHOUSE_SOURCE_DATE_COLUMNS.getName(), CLICKHOUSE_SOURCE_DATE_COLUMNS);
        put(CLICKHOUSE_SOURCE_DATE_FORMAT.getName(), CLICKHOUSE_SOURCE_DATE_FORMAT);
        put(CLICKHOUSE_OPTIMIZE.getName(), CLICKHOUSE_OPTIMIZE);
    }};

}
