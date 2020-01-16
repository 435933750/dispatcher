package com.lx.supplement.bean;

import com.lx.supplement.config.DataBaseConfig;
import com.lx.supplement.config.TableConfig;
import com.lx.util.StringUtils;
import org.apache.commons.collections.CollectionUtils;
import scala.Serializable;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static scala.collection.JavaConverters.asScalaBufferConverter;

/**
 * 从binlog-kafka里消费到的数据
 */
public class ReceiveBean implements Serializable {


    public ReceiveBean(String json) {
        this.json = json;
    }

    public ReceiveBean() {
    }

    private String database;
    private String table;
    private Timestamp ts;
    private String type;
    private List<String> pkNames;
    private List<Map<String, String>> old;
    private List<Map<String, String>> data;
    private String json;//存储错误
    private TableConfig config;
    private String pk;

    /**
     * 校验该条数据是否合法
     *
     * @return
     */
    public Boolean isValid() {
        return (!StringUtils.isEmpty(database, table, ts, type)) && (!CollectionUtils.isEmpty(data)) && (!CollectionUtils.isEmpty(pkNames));
    }



    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Map<String, String>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, String>> old) {
        this.old = old;
    }

    public List<Map<String, String>> getData() {
        return data;
    }

    public void setData(List<Map<String, String>> data) {
        this.data = data;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public TableConfig getConfig() {
        return config;
    }

    public void setConfig(TableConfig config) {
        this.config = config;
    }
}