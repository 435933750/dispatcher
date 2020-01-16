package com.lx.supplement.config;

import com.alibaba.fastjson.JSONArray;
import com.lx.util.PropertiesUtils;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataBaseConfig implements Serializable {


    private String dataBase;
    private List<TableConfig> tables;
    private Map<String, TableConfig> tableMaps;


    /**
     * 初始化时封装一个map对象，方便查询
     *
     * @param
     * @return
     */
    public static Map<String, DataBaseConfig> createDatBaseConf() {
        JSONArray array = PropertiesUtils.getJsonResourceArray("suppleConfig.json");
        Map<String, DataBaseConfig> map = new HashMap<>();
        List<DataBaseConfig> dbcs = array.toJavaList(DataBaseConfig.class);
        for (DataBaseConfig dbc : dbcs) {
            dbc.tableMaps = new HashMap<>();
            if (CollectionUtils.isEmpty(dbc.tables)) {
                return null;
            }
            for (TableConfig table : dbc.tables) {
                dbc.tableMaps.put(table.getName(), table);
            }
            map.put(dbc.getDataBase(), dbc);
        }
        return map;
    }


    public TableConfig getConfig(String table) {
        return tableMaps.get(table);
    }


    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public List<TableConfig> getTables() {
        return tables;
    }

    public void setTables(List<TableConfig> tables) {
        this.tables = tables;
    }

    public Map<String, TableConfig> getTableMaps() {
        return tableMaps;
    }

    public void setTableMaps(Map<String, TableConfig> tableMaps) {
        this.tableMaps = tableMaps;
    }


}
