package com.lx.supplement.config;

public enum HandleEnum {
    UPDATE, SUM, CUSTOM;

    public static HandleEnum get(String s) {
        if (UPDATE.name().equals(s)) {
            return UPDATE;
        } else if (SUM.name().equals(s)) {
            return SUM;
        } else if (CUSTOM.name().equals(s)) {
            return CUSTOM;
        }
        return null;
    }
}
