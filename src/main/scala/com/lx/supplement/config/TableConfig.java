package com.lx.supplement.config;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class TableConfig implements Serializable {
    private String pk;
    private String database;
    private String name;
    private String output;
    private List<ActionEnum> action;
    private Map<HandleEnum, Map<String, String>> handle;

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public List<ActionEnum> getAction() {
        return action;
    }

    public void setAction(List<ActionEnum> action) {
        this.action = action;
    }

    public Map<HandleEnum, Map<String, String>> getHandle() {
        return handle;
    }

    public void setHandle(Map<HandleEnum, Map<String, String>> handle) {
        this.handle = handle;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }
}
