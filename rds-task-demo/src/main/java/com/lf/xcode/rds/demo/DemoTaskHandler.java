package com.lf.xcode.rds.demo;

import com.lf.xcode.rds.demo.base.BaseUtil;
import com.lf.xcode.rds.demo.base.RedisClient;
import com.lf.xcode.rds.task.rdsbatch.RdsBatchTaskHandler;
import com.lf.xcode.rds.task.rdsbatch.param.InvokeParam;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.redis.util.RedisLockRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 异步任务demo
 */
@Slf4j
@Component
public class DemoTaskHandler {


    //主任务key
    private static final String MAIN_KEY = "myMainTask";
    //子任务前缀
    private static final String PREFIX = "mySubTask";
    //分布式锁名称
    private static final String POD_LOCK_KEY = "MY_POD_LOCK_KEY";

    //分布式任务处理器
    @Getter
    private final RdsBatchTaskHandler rdsBatchTaskHandler;
    //分布式锁
    private final RedisLockRegistry redisLockRegistry;
    //业务执行方法
    private Function<InvokeParam, Boolean> bizInvokeFunc;


    public DemoTaskHandler(RedisLockRegistry redisLockRegistry, RedisClient redisClient, DemoRdsConfig config) {
        this.redisLockRegistry = redisLockRegistry;
        //初始化分布式任务处理器
        this.rdsBatchTaskHandler = new RdsBatchTaskHandler(redisClient::getJedis, generateLock(config), config,
                () -> bizInvokeFunc, MAIN_KEY, PREFIX);
    }

    /**
     * 注册业务执行的方法
     *
     * @param bizFunction
     */
    public void registerBizInvoke(Function<InvokeParam, Boolean> bizFunction) {
        this.bizInvokeFunc = bizFunction;
    }

    /**
     * 任务调度
     */
    @Scheduled(cron = "*/10 * * * * ?")
    public void schedule() {
        //保证只有一个pod执行
        BaseUtil.safeOnePodDo(redisLockRegistry, POD_LOCK_KEY, rdsBatchTaskHandler::mainLoop, 3 * 1000);
    }


    /**
     * 添加子任务
     */
    public void addSubTask(String mainTaskId, String subTaskId) {
        rdsBatchTaskHandler.addSubTask(mainTaskId, subTaskId);
    }

    /**
     * 添加子任务
     *
     * @param mainTaskId
     * @param subTaskId
     */
    public void addSubTask(String mainTaskId, List<String> subTaskId) {
        rdsBatchTaskHandler.addSubTask(mainTaskId, subTaskId);
    }

    /**
     * 添加主任务
     *
     * @param mainTaskId
     */
    public void addMainTask(String mainTaskId) {
        rdsBatchTaskHandler.addMainTask(mainTaskId);
    }

    /**
     * 获取主任务当前数量
     */
    public long getMainTaskFreeSize() {
        return rdsBatchTaskHandler.mainTaskFreeSize();
    }

    /**
     * 生成分布式锁的回调执行方法
     *
     * @param config
     * @return
     */
    private Function<Supplier<Boolean>, Boolean> generateLock(DemoRdsConfig config) {
        return action -> BaseUtil.safeOnePodDo(redisLockRegistry, MAIN_KEY, action, config.getMainLoopLockTimeout());
    }


    /**
     * 清理所有的队列（主任务队列+子任务队列+执行队列）
     */
    public void clearAll() {
        rdsBatchTaskHandler.clearAll();
    }

}
