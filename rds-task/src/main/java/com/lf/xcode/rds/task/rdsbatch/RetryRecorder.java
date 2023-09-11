package com.lf.xcode.rds.task.rdsbatch;

import cn.hutool.core.util.ObjectUtil;
import com.lf.xcode.rds.task.rdsbatch.config.BatchTaskConfig;
import com.lf.xcode.rds.task.rdstask.RdsZsetTask;
import redis.clients.jedis.Jedis;

import java.util.function.Supplier;

/**
 * 任务重试记录
 */
public class RetryRecorder {

    private String prefix;
    private BatchTaskConfig config;
    private Supplier<Jedis> jedisSupplier;

    public RetryRecorder(BatchTaskConfig config, String prefix, Supplier<Jedis> jedisSupplier) {
        this.config = config;
        this.prefix = prefix;
        this.jedisSupplier = jedisSupplier;
    }

    private static final String RETRY_COUNT_RECORD = "RETRY_RECORD";

    protected String generateRetryRecordKey(String mainTask, String subTask) {
        return RdsZsetTask.XCODE_RDS_TASK_RDS_ZSET_TASK + prefix + RETRY_COUNT_RECORD + ":" + mainTask + ":" + subTask;
    }

    /**
     * 重试次数+1
     *
     * @param subTask
     */
    protected void addRetryRecord(String mainTask, String subTask) {
        String key = generateRetryRecordKey(mainTask, subTask);
        try (Jedis jedis = jedisSupplier.get()) {
            jedis.incr(key);
            //留足够的记录时间
            jedis.pexpireAt(key, System.currentTimeMillis() + config.getTaskTimeout() * (config.getRetryCount() + 1));
        }
    }

    /**
     * 获取重试次数
     *
     * @param subTask
     * @return
     */
    protected int getRetryCount(String mainTask, String subTask) {
        String key = generateRetryRecordKey(mainTask, subTask);
        try (Jedis jedis = jedisSupplier.get()) {
            String count = jedis.get(key);
            return ObjectUtil.isEmpty(count) ? 0 : Integer.parseInt(count);
        }
    }

    /**
     * 删除重试记录
     *
     * @param subTask
     */
    protected void delRetryRecord(String mainTask, String subTask) {
        String key = generateRetryRecordKey(mainTask, subTask);
        try (Jedis jedis = jedisSupplier.get()) {
            jedis.del(key);
        }
    }
}
