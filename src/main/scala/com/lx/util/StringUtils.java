package com.lx.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;


public class StringUtils {

    static Logger logger = LoggerFactory.getLogger(StringUtils.class);

    public static boolean isEmpty(Object... values) {
        if (values == null) {
            return true;
        }
        for (Object value : values) {
            if (value == null || value.toString()=="" || value.toString().trim().length()<1) {
                return true;
            }
        }
        return false;
    }


    public static String timestamp2String(Long time, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        Instant instant = new Date(time).toInstant();
        return formatter.format(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));
    }

}
