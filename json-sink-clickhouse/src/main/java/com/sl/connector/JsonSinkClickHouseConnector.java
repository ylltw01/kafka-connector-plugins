package com.sl.connector;

import com.sl.connector.clickhouse.ClickHouseConfigValidator;
import com.sl.connector.model.ConfigField;
import com.sl.connector.utils.ModuleUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.sl.connector.clickhouse.ClickHouseConfigField.CLICKHOUSE_CONFIG_MAP;
import static com.sl.connector.constant.CommonConstant.GROUP_NAME;

/**
 * @author L
 */
public class JsonSinkClickHouseConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(JsonSinkClickHouseTask.class);
    private Map<String, String> props;

    @Override
    public String version() {
        return ModuleUtils.version("build.properties");
    }

    @Override
    public void start(Map<String, String> props) {
        for (Map.Entry<String, String> entry : props.entrySet()) {
            logger.info("input param key: {}, value: {}", entry.getKey(), entry.getValue());
        }
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JsonSinkClickHouseTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {
        logger.info("stop Json Sink ClickHouse Connector");
    }

    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();
        for (Map.Entry<String, ConfigField> entry : CLICKHOUSE_CONFIG_MAP.entrySet()) {
            ConfigField field = entry.getValue();
            config.define(field.getName(), field.getType(), field.getDefaultValue(), null, field.getImportance(), field.getDesc(),
                    GROUP_NAME, 1, field.getWidth(), field.getName(), field.getDependents(), field.getRecommender());
        }
        return config;
    }

    @Override
    public Config validate(Map<String, String> props) {
        Map<String, ConfigValue> configMap = new HashMap<>();
        ClickHouseConfigValidator validator = new ClickHouseConfigValidator();
        validator.baseValidate(configMap, CLICKHOUSE_CONFIG_MAP, props);
        validator.clickHouseValidate(configMap, props);
        validator.dateformatValidate(configMap, props);
        return new Config(new LinkedList<>(configMap.values()));
    }

}
