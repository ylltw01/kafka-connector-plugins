package com.sl.connector.model;

import org.apache.kafka.common.config.ConfigDef;

import java.util.List;

/**
 * @author L
 */
public class ConfigField {

    private String name;

    private String displayName;

    private String desc;

    private Object defaultValue;

    private ConfigDef.Width width;

    private ConfigDef.Type type;

    private ConfigDef.Importance importance;

    private List<String> dependents;

    private ConfigDef.Recommender recommender;

    private boolean require;

    public ConfigField() {
    }

    public ConfigField(String name, String desc, ConfigDef.Type type, ConfigDef.Importance importance, boolean require) {
        this.name = name;
        this.desc = desc;
        this.type = type;
        this.importance = importance;
        this.require = require;
    }

    public ConfigField(String name, String desc, Object defaultValue, ConfigDef.Type type, ConfigDef.Importance importance, boolean require) {
        this.name = name;
        this.desc = desc;
        this.defaultValue = defaultValue;
        this.type = type;
        this.importance = importance;
        this.require = require;
    }

    public ConfigField(String name, String displayName, String desc, Object defaultValue, ConfigDef.Width width, ConfigDef.Type type,
                       ConfigDef.Importance importance, List<String> dependents, ConfigDef.Recommender recommender, boolean require) {
        this.name = name;
        this.displayName = displayName;
        this.desc = desc;
        this.defaultValue = defaultValue;
        this.width = width;
        this.type = type;
        this.importance = importance;
        this.dependents = dependents;
        this.recommender = recommender;
        this.require = require;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public ConfigDef.Width getWidth() {
        return width;
    }

    public void setWidth(ConfigDef.Width width) {
        this.width = width;
    }

    public ConfigDef.Type getType() {
        return type;
    }

    public void setType(ConfigDef.Type type) {
        this.type = type;
    }

    public ConfigDef.Importance getImportance() {
        return importance;
    }

    public void setImportance(ConfigDef.Importance importance) {
        this.importance = importance;
    }

    public List<String> getDependents() {
        return dependents;
    }

    public void setDependents(List<String> dependents) {
        this.dependents = dependents;
    }

    public ConfigDef.Recommender getRecommender() {
        return recommender;
    }

    public void setRecommender(ConfigDef.Recommender recommender) {
        this.recommender = recommender;
    }

    public boolean isRequire() {
        return require;
    }

    public void setRequire(boolean require) {
        this.require = require;
    }
}
