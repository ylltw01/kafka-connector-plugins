package com.sl.connector.model;

import java.util.HashSet;
import java.util.Set;

/**
 * @author L
 */
public class ClickHouseSinkData {

    private String table;

    private String localTable;

    private boolean optimize;

    private String columns;

    private Set<String> values = new HashSet<>();

    public ClickHouseSinkData() {
    }

    public ClickHouseSinkData(String table, String localTable, boolean optimize) {
        this.table = table;
        this.localTable = localTable;
        this.optimize = optimize;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getLocalTable() {
        return localTable;
    }

    public void setLocalTable(String localTable) {
        this.localTable = localTable;
    }

    public boolean isOptimize() {
        return optimize;
    }

    public void setOptimize(boolean optimize) {
        this.optimize = optimize;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public Set<String> getValues() {
        return values;
    }

    public void setValues(Set<String> values) {
        this.values = values;
    }

    public void putValue(Set<String> values) {
        this.values.addAll(values);
    }

    public void putValue(String values) {
        this.values.add(values);
    }
}
