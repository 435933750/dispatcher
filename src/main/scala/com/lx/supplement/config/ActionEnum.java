package com.lx.supplement.config;

public enum ActionEnum {
    INSERT, UPDATE;

    public static ActionEnum get(String s) {
        if (INSERT.name().equals(s)) {
            return INSERT;
        } else if (UPDATE.name().equals(s)) {
            return UPDATE;
        }
        return null;
    }
}
