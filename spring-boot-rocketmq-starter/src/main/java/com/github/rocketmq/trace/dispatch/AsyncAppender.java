package com.github.rocketmq.trace.dispatch;


public interface AsyncAppender {
    /**
     *编码数据上下文到缓冲区
     * @param context
     */
    void append(Object context);

    /**
     * 实际写数据操作
     */
    void flush();
}
