package com.yuxin.kvlsmtree.utils;

import com.sun.org.slf4j.internal.Logger;
public class LoggerUtil {

    public static void debug(Logger logger, String format, Object... arguments) {
        if (logger.isDebugEnabled()) {
            logger.debug(format, arguments);
        }
    }

    public static void info(Logger logger, String format, Object... arguments) {
        if (logger.isInfoEnable()) {
            logger.info(format, arguments);
        }
    }

    public static void error(Logger logger, Throwable t, String format, Object... arguments) {
        if (logger.isErrorEnabled()) {
            logger.error(format, arguments, t);
        }
    }
}
