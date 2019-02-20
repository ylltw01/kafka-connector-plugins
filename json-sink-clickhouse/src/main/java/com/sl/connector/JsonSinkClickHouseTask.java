package com.sl.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.sl.connector.clickhouse.ClickHouseHelper;
import com.sl.connector.clickhouse.ClickHouseTypeConvert;
import com.sl.connector.jdbc.JdbcConnectConfig;
import com.sl.connector.jdbc.JdbcDataSource;
import com.sl.connector.model.ClickHouseSinkData;
import com.sl.connector.model.ClickHouseTableInfo;
import com.sl.connector.utils.ModuleUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.sl.connector.clickhouse.ClickHouseConfigField.*;
import static com.sl.connector.constant.CommonConstant.BOOLEAN_TRUE;

/**
 * @author L
 */
public class JsonSinkClickHouseTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(JsonSinkClickHouseTask.class);
    private ObjectMapper mapper = new ObjectMapper();
    private ClickHouseHelper clickHouseHelper = new ClickHouseHelper();
    private JdbcDataSource dataSource;
    private String database;
    private String topic;
    private ClickHouseTableInfo tableInfo;
    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public String version() {
        return ModuleUtils.version("build.properties");
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(TOPIC.getName());
        String hosts = props.get(CLICKHOUSE_HOSTS.getName());
        String port = props.get(CLICKHOUSE_JDBC_PORT.getName());
        String user = props.get(CLICKHOUSE_JDBC_USER.getName());
        String password = props.get(CLICKHOUSE_JDBC_PASSWORD.getName());
        database = props.get(CLICKHOUSE_SINK_DATABASE.getName());
        JdbcConnectConfig connectConfig = JdbcConnectConfig.initCkConnectConfig(hosts, port, user, password);
        // 初始化与 ClickHouse 连接
        dataSource = new JdbcDataSource(connectConfig);
        String table = props.get(CLICKHOUSE_SINK_TABLES.getName());
        String localTable = props.get(CLICKHOUSE_SINK_LOCAL_TABLES.getName());
        String sinkDateCol = props.get(CLICKHOUSE_SINK_DATE_COLUMNS.getName());
        String sourceDateCol = props.get(CLICKHOUSE_SOURCE_DATE_COLUMNS.getName());
        String dateformat = props.get(CLICKHOUSE_SOURCE_DATE_FORMAT.getName());
        String optimize = props.get(CLICKHOUSE_OPTIMIZE.getName());
        SimpleDateFormat sdf = new SimpleDateFormat(dateformat);
        boolean optimizeBool = BOOLEAN_TRUE.equalsIgnoreCase(optimize);
        // 初始化写入的 ClickHouse 数据表信息
        tableInfo = new ClickHouseTableInfo(table, localTable, sinkDateCol, sourceDateCol, optimizeBool, sdf);
        logger.info("start json sink ClickHouse task, 开始连接ClickHouse");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        Map<String, ClickHouseSinkData> insertMap = new HashMap<>(16);
        try {
            Map<String, String> columnMaps = dataSource.descTableColType(String.format("`%s`.`%s`", database, tableInfo.getTable()));
            for (SinkRecord record : records) {
                logger.info("消费的offset: " + record.kafkaOffset());
                try {
                    ClickHouseSinkData sinkData = handlerRecord(columnMaps, record);
                    clickHouseHelper.collectInsertData(insertMap, sinkData);
                } catch (IOException e) {
                    logger.error("消费到的数据不能被json解析, 数据: " + record.value(), e);
                }
            }
            clickHouseHelper.batchInsert(dataSource, database, insertMap);
        } catch (SQLException e) {
            logger.error("执行ClickHouse SQL失败,", e);
            throw new ConfigException(e.getMessage());
        } catch (ParseException e) {
            logger.error("日期格式解析错误,", e);
            throw new ConfigException(e.getMessage());
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void stop() {
        if (dataSource != null) {
            try {
                dataSource.close();
                logger.info("关闭ClickHouse连接成功");
            } catch (SQLException e) {
                logger.error("关闭ClickHouse连接失败, ", e);
            }
        }
    }

    /**
     * 主要处理消费到的 Record 信息的逻辑, 通过 Jackson 解析 json 数据, 与表字段做比对,
     * 如果消费到 json 中有字段不存在表中, 为避免数据丢失将会抛出异常, 任务失败.
     * 将解析到的字段与字段数据已经表信息, 封装至 ClickHouseSinkData 对象返回
     *
     * @param columnMaps key 为字段名 value为字段类型
     * @param record     SinkRecord
     * @return ClickHouseSinkData
     * @throws IOException    解析 json 数据失败, 将会抛出该异常
     * @throws ParseException 解析日期格式失败, 将会抛出该异常
     */
    private ClickHouseSinkData handlerRecord(Map<String, String> columnMaps, SinkRecord record) throws IOException, ParseException {
        ClickHouseSinkData sinkData = new ClickHouseSinkData(tableInfo.getTable(), tableInfo.getLocalTable(), tableInfo.isOptimize());
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        JsonNode jsonNode = mapper.readTree((String) record.value());
        Iterator<String> keys = jsonNode.fieldNames();
        while (keys.hasNext()) {
            String fieldName = keys.next();
            String colType = columnMaps.get(fieldName);
            if (colType != null) {
                columns.add("`" + fieldName + "`");
                JsonNode nodeValue = jsonNode.get(fieldName);
                if (ClickHouseTypeConvert.isNumberType(colType)) {
                    values.add(nodeValue.asText());
                } else if (nodeValue instanceof ContainerNode) {
                    values.add(String.format("'%s'", nodeValue.toString().replaceAll("'", "\\\\'")));
                } else {
                    values.add(String.format("'%s'", nodeValue.asText().replaceAll("'", "\\\\'")));
                }
            } else {
                throw new ConfigException(String.format("topic: %s, 列: %s, 不存在ClickHouse表: %s中, 该表所有列: %s",
                        topic, fieldName, tableInfo.getTable(), StringUtils.join(columnMaps.keySet(), ", ")));
            }
        }
        parsePartitionDateInfo(columns, values, jsonNode);
        sinkData.setColumns(StringUtils.join(columns, ", "));
        sinkData.putValue(String.format("(%s)", StringUtils.join(values, ", ")));
        return sinkData;
    }

    /**
     * 用于解析 ClickHouse 中的分区字段和分区字段值
     */
    private void parsePartitionDateInfo(List<String> columns, List<Object> values, JsonNode jsonNode) throws ParseException {
        columns.add("`" + tableInfo.getSinkDateCol() + "`");
        if (StringUtils.isNotEmpty(tableInfo.getSourceDateCol())) {
            JsonNode sourceDateNode = jsonNode.get(tableInfo.getSourceDateCol());
            if (sourceDateNode != null) {
                LocalDateTime time = LocalDateTime.ofInstant(tableInfo.getSdf().parse(sourceDateNode.asText()).toInstant(), ZoneId.systemDefault());
                values.add(String.format("'%s'", dateTimeFormatter.format(time)));
            } else {
                values.add(String.format("'%s'", dateTimeFormatter.format(LocalDateTime.now())));
            }
        } else {
            values.add(String.format("'%s'", dateTimeFormatter.format(LocalDateTime.now())));
        }
    }

}
