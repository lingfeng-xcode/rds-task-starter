package com.lf.xcode.rds.task.rdsbatch.config;


import com.lf.xcode.rds.task.constant.TaskType;
import com.lf.xcode.rds.task.rdsbatch.AllocationPolicy;
import com.lf.xcode.rds.task.rdstask.RdsZsetTask;
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
    private static final int DEFAULT_EXECUTE_TASK_LIMIT = 10;
    private static final int DEFAULT_SUB_TASK_LIMIT = 50;
    private static final int DEFAULT_MAIN_TASK_LIMIT = 10;
    private static final int DEFAULT_RETRY_COUNT = 3;
    private static final long DEFAULT_TASK_TIMEOUT = 1000 * 60 * 60 * 24L;
    private static final long DEFAULT_TASK_EXPIRE_TIME = RdsZsetTask.NO_EXPIRE;
    private static final int DEFAULT_MAIN_LOOP_SIZE = 10;
    private static final AllocationPolicy DEFAULT_ALLOCATION_POLICY = AllocationPolicy.RANDOM;
    private static final TaskType DEFAULT_TASK_TYPE = TaskType.SUB_TASK_NO_LIMIT;
    private static final boolean DEFAULT_USE_THREAD_POOL = false;
    private static final int DEFAULT_THREAD_POOL_SIZE = 10;


    //同时执行任务的限制
    private volatile int executeTaskLimit;
    //主任务的限制
    private volatile int mainTaskLimit;
    //子任务数量限制
    private volatile int subTaskLimit;
    //任务重试次数
    private volatile int retryCount;
    //任务执行超时时间 单位ms
    private volatile long taskTimeout;
    //主任务过期时间
    private volatile long mainTaskExpireTime;
    //子任务过期时间
    private volatile long subTaskExpireTime;
    //主任务列表的大小
    private volatile int mainLoopSize;
    //分配策略:随机/顺序 分配主任务时的策略
    private volatile AllocationPolicy allocationPolicy;
    //任务类型
    private TaskType taskType;
    //是否使用线程池执行业务方法
    private volatile boolean useBizThreadPool;
    //执行业务方法的线程池大小
    private int bizThreadPoolSize;
    //主任务分布式锁超时时间,用于防止主任务执行时间不够，其他线程再次执行
    private long mainLoopLockTimeout;

    public BatchTaskConfig() {
        this.executeTaskLimit = DEFAULT_EXECUTE_TASK_LIMIT;
        this.mainTaskLimit = DEFAULT_MAIN_TASK_LIMIT;
        this.subTaskLimit = DEFAULT_SUB_TASK_LIMIT;
        this.retryCount = DEFAULT_RETRY_COUNT;
        this.taskTimeout = DEFAULT_TASK_TIMEOUT;
        this.mainLoopSize = DEFAULT_MAIN_LOOP_SIZE;
        this.allocationPolicy = DEFAULT_ALLOCATION_POLICY;
        this.taskType = DEFAULT_TASK_TYPE;
        this.useBizThreadPool = DEFAULT_USE_THREAD_POOL;
        this.bizThreadPoolSize = DEFAULT_THREAD_POOL_SIZE;
        this.subTaskExpireTime = DEFAULT_TASK_EXPIRE_TIME;
        this.mainTaskExpireTime = DEFAULT_TASK_EXPIRE_TIME;
    }
}
