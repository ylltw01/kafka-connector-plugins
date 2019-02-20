package com.sl.connector.model;

import java.text.SimpleDateFormat;

/**
 * @author L
 */
public class ClickHouseTableInfo {

    private String table;

    private String localTable;

    private String sinkDateCol;

    private String sourceDateCol;

    private boolean optimize;

    private SimpleDateFormat sdf;

    public ClickHouseTableInfo() {
    }

    public ClickHouseTableInfo(String table, String localTable, String sinkDateCol, String sourceDateCol, boolean optimize, SimpleDateFormat sdf) {
        this.table = table;
        this.localTable = localTable;
        this.sinkDateCol = sinkDateCol;
        this.sourceDateCol = sourceDateCol;
        this.optimize = optimize;
        this.sdf = sdf;
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

    public String getSinkDateCol() {
        return sinkDateCol;
    }

    public void setSinkDateCol(String sinkDateCol) {
        this.sinkDateCol = sinkDateCol;
    }

    public String getSourceDateCol() {
        return sourceDateCol;
    }

    public void setSourceDateCol(String sourceDateCol) {
        this.sourceDateCol = sourceDateCol;
    }

    public SimpleDateFormat getSdf() {
        return sdf;
    }

    public void setSdf(SimpleDateFormat sdf) {
        this.sdf = sdf;
    }

    public boolean isOptimize() {
        return optimize;
    }

    public void setOptimize(boolean optimize) {
        this.optimize = optimize;
    }
}
