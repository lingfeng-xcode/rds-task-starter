package com.lf.xcode.rds.util;


import cn.hutool.core.util.ObjectUtil;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtil {
    private static final String PREFIX = "com.shuwen";
    private static final int EXCEPTION_LEVEL = 5;


    public static String getMessage(Throwable e) {
        return getMessage(e, 5);
    }

    /**
     * @param e           throwable
     * @param messageLine 取异常到行数 default 5
     * @return
     */
    public static String getMessage(Throwable e, int messageLine) {

        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String[] errs = sw.toString().split("\\n\\t");
        StringBuffer sb = new StringBuffer();
        //层级
        int level = 0;
        //返回报错的最深栈
        for (String err : errs) {
            if (level == 0) {
                level++;
                sb.append(err);
                continue;
            }
            if (err.contains(PREFIX)) {
                level++;
                sb.append(err);
            }
            if (level >= messageLine) {
                break;
            }
        }
        if (ObjectUtil.isEmpty(sb)) {
            return e.getMessage();
        }
        return sb.toString();
    }

    /**
     * 不过框架异常类
     * @param e           throwable
     * @param messageLine 取异常到行数 default 5
     * @return
     */
    public static String getMessageFrame(Throwable e, int messageLine) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String[] errs = sw.toString().split("\\n\\t");
        StringBuffer sb = new StringBuffer();
        //层级
        int level = 0;
        //返回报错的最深栈
        for (String err : errs) {
            if (level == 0) {
                level++;
                sb.append(err);
                continue;
            }
            level++;
            sb.append(err);
            if (level >= messageLine) {
                break;
            }
        }
        if (ObjectUtil.isEmpty(sb)) {
            return e.getMessage();
        }
        return sb.toString();
    }
}
