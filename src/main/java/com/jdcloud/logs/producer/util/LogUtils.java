package com.jdcloud.logs.producer.util;

import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.util.HashMap;
import java.util.Map;

/**
 * log utils
 *
 * @author liubai
 * @date 2022/7/15
 */
public class LogUtils {

    /**
     * 日志收敛开关，调试模式下有些频繁打印的日志可以做收敛
     */
    public static boolean LOG_CONVERGENCE = false;

    /**
     * 日志收敛时间，调试模式下频繁打印的日志的收敛时间
     */
    public static int CONVERGENCE_MILLIS = 30000;

    /**
     * 收敛时间map: Map<logger, 上一次发送时间>
     */
    private static final Map<Logger, Long> CONVERGENCE_MAP = new HashMap<Logger, Long>(16);

    public static void message(Level level, Logger logger, String format, Object... args) {
        message(level, logger, false, format, null, args);
    }

    public static void message(Level level, Logger logger, String format, Throwable e, Object... args) {
        message(level, logger, false, format, e, args);
    }

    public static void message(Level level, Logger logger, boolean convergence, String format, Object... args) {
        message(level, logger, convergence, format, null, args);
    }

    public static void message(Level level, Logger logger, boolean convergence, String format, Throwable e, Object... args) {
        if (convergence && LOG_CONVERGENCE) {
            Long last = CONVERGENCE_MAP.get(logger);
            long now = System.currentTimeMillis();
            if (last == null || now >= last + CONVERGENCE_MILLIS) {
                CONVERGENCE_MAP.put(logger, now);
                messageInternal(level, logger, format, e, args);
            }
        } else {
            messageInternal(level, logger, format, e, args);
        }
    }

    private static void messageInternal(Level level, Logger logger, String format, Throwable e, Object... args) {
        switch (level) {
            case TRACE:
                logger.trace(format, args);
                break;
            case DEBUG:
                logger.debug(format, args);
                break;
            case INFO:
                logger.info(format, args);
                break;
            case WARN:
                if (e == null) {
                    logger.warn(format, args);
                } else {
                    logger.warn(format, e);
                }
                break;
            case ERROR:
                if (e == null) {
                    logger.error(format, args);
                } else {
                    logger.error(format, e);
                }
                break;
            default:
                break;
        }
    }
}
