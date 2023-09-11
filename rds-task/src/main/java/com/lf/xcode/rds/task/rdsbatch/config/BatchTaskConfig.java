package com.lf.xcode.rds.task.rdsbatch.config;


import com.lf.xcode.rds.task.rdsbatch.AllocationPolicy;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 任务配置 包含限流参数
 */
@Slf4j
@Getter
@Setter
public class BatchTaskConfig {
    private static final int DEFAULT_SUB_TASK_LIMIT = 1000;
    private static final int DEFAULT_MAIN_TASK_LIMIT = 1000;
    private static final int DEFAULT_RETRY_COUNT = 3;
    private static final long DEFAULT_TASK_TIMEOUT = 1000 * 60 * 60 * 24;
    private static final int DEFAULT_MAIN_LOOP_SIZE = 10;
    private static final AllocationPolicy DEFAULT_ALLOCATION_POLICY = AllocationPolicy.RANDOM;
    private static final boolean DEFAULT_USE_THREAD_POOL = true;
    private static final int DEFAULT_THREAD_POOL_SIZE = 10;

    //同时执行任务的限制
    private volatile int subTaskLimit;
    //主任务的限制
    private volatile int mainTaskLimit;
    //任务重试次数
    private volatile int retryCount;
    //任务执行超时时间 单位ms
    private volatile long taskTimeout;
    //主任务列表的大小
    private volatile int mainLoopSize;
    //分配策略:随机/顺序 分配主任务时的策略
    private volatile AllocationPolicy allocationPolicy;
    //是否使用线程池执行
    private volatile boolean useThreadPool;
    //执行业务方法的线程池大小
    private int threadPoolSize;
    //主任务分布式锁超时时间,用于防止主任务执行时间不够，其他线程再次执行
    private int mainLoopLockTimeout;

    public BatchTaskConfig() {
        this.subTaskLimit = DEFAULT_SUB_TASK_LIMIT;
        this.mainTaskLimit = DEFAULT_MAIN_TASK_LIMIT;
        this.retryCount = DEFAULT_RETRY_COUNT;
        this.taskTimeout = DEFAULT_TASK_TIMEOUT;
        this.mainLoopSize = DEFAULT_MAIN_LOOP_SIZE;
        this.allocationPolicy = DEFAULT_ALLOCATION_POLICY;
        this.useThreadPool = DEFAULT_USE_THREAD_POOL;
        this.threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    }
}
