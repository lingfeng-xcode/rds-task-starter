package com.lf.xcode.rds.task.rdsbatch;

import com.lf.xcode.rds.task.constant.TaskType;
import com.lf.xcode.rds.task.rdsbatch.config.BatchTaskConfig;
import com.lf.xcode.rds.task.rdsbatch.param.InvokeParam;
import com.lf.xcode.rds.task.rdstask.RdsQueue;
import com.lf.xcode.rds.task.rdstask.RdsZsetTask;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public abstract class AbstractBatchTaskHandler<S> implements BatchTaskHandler<String, S> {
    protected Supplier<Jedis> jedisSupplier;
    protected Function<Supplier<Boolean>, Boolean> distributedLock;
    protected BatchTaskConfig config;

    //子任务
    protected final RdsZsetTask rdsSubTask;
    //主任务
    protected final RdsZsetTask rdsMainTask;
    //执行队列
    protected final RdsQueue executeQueue;
    //重试记录器
    protected final RetryRecorder retryRecorder;
    //业务处理器
    protected final Supplier<Function<InvokeParam, Boolean>> bizHandler;

    //父任务-默认优先级
    protected final static double DEFAULT_PRIORITY = 1;
    //父任务-高优先级（进行中的任务）
    protected final static double DEFAULT_PRIORITY_HIGH = 0;
    //子任务状态：等待执行
    protected final static double SUB_TASK_WAIT = 0;
    //子任务状态: 执行中
    protected final static double SUB_TASK_RUNNING = 1;
    //所有任务的前缀
    protected final String prefix;
    //主任务的key
    protected final String mainTaskName;

    protected ExecutorService bizThreadPool;

    AbstractBatchTaskHandler(Supplier<Jedis> jedisSupplier,
                             Function<Supplier<Boolean>, Boolean> distributedLock,
                             BatchTaskConfig config,
                             Supplier<Function<InvokeParam, Boolean>> bizHandler,
                             String mainKey, String prefix) {
        this.jedisSupplier = jedisSupplier;
        this.distributedLock = distributedLock;
        this.config = config;
        this.bizHandler = bizHandler;
        this.mainTaskName = mainKey;
        this.prefix = prefix;
        //主任务列表
        rdsMainTask = new RdsZsetTask(prefix + ":MAIN_TASK", jedisSupplier, config::getMainTaskLimit, config::getMainTaskExpireTime);
        //子任务limit
        Supplier<Integer> subLimitSupplier = config.getTaskType() == TaskType.SUB_TASK_NO_LIMIT ? () -> Integer.MAX_VALUE : config::getSubTaskLimit;
        //子任务列表
        rdsSubTask = new RdsZsetTask(prefix + ":SUB_TASK", jedisSupplier, subLimitSupplier, config::getSubTaskExpireTime);
        //执行队列
        executeQueue = new RdsQueue(prefix + ":EXECUTE", jedisSupplier, distributedLock, config::getExecuteTaskLimit, config::getTaskTimeout);
        //重试记录
        retryRecorder = new RetryRecorder(config, prefix, jedisSupplier);
        //线程池初始化
        if (config.isUseBizThreadPool()) {
            bizThreadPool = Executors.newFixedThreadPool(config.getBizThreadPoolSize());
        }
        log.info("{} init", this);
    }

    protected abstract void preHandle();

    protected abstract void mainHandle();

    protected abstract void postHandle();


    /**
     * 提高主任务优先级
     *
     * @param mainTask
     */
    protected void upgradeMainTaskPriority(String mainTask) {
        rdsMainTask.set(mainTaskName, mainTask, DEFAULT_PRIORITY_HIGH);
    }

    /**
     * 删除指定主任务
     *
     * @param mainTask
     */
    protected void delMainTask(String mainTask) {
        rdsMainTask.remove(mainTaskName, mainTask);
    }


    /**
     * 添加子任务
     *
     * @param mainTask
     * @param subTask
     * @return
     */
    public boolean addSubTask(String mainTask, String subTask) {
        return rdsSubTask.add(mainTask, subTask, SUB_TASK_WAIT);
    }


    /**
     * 添加子任务
     *
     * @param mainTask
     * @param subTaskList
     * @return
     */
    public boolean addSubTask(String mainTask, Collection<String> subTaskList) {
        return rdsSubTask.add(mainTask, subTaskList, SUB_TASK_WAIT);
    }

    /**
     * 添加一个主任务
     *
     * @param mainTask
     * @return
     */
    public boolean addMainTask(String mainTask) {
        //默认低优先级
        return rdsMainTask.add(mainTaskName, mainTask, DEFAULT_PRIORITY);
    }

    /**
     * 主任务小于限制数量
     * 用于在捞取媒资的时候进行 限流
     *
     * @return
     */

    public boolean mainTaskHasFree() {
        return rdsMainTask.size(mainTaskName) < config.getMainTaskLimit();
    }

    /**
     * 主任务剩余数量
     *
     * @return
     */
    @Override
    public int mainTaskFreeSize() {
        return config.getMainTaskLimit() - rdsMainTask.size(mainTaskName);
    }

    /**
     * 清理所有的主任务
     *
     * @return
     */
    public boolean clearMainTask() {
        rdsMainTask.clear(mainTaskName);
        return true;
    }


    /**
     * 清理所有的队列
     *
     * @return
     */
    public boolean clearAll() {
        Set<String> mainTaskList;
        do {
            int size = rdsMainTask.size(mainTaskName);
            log.info("mainTaskList size={}", size);
            mainTaskList = rdsMainTask.poll(mainTaskName, 0, -1, 0, 100);
            if (mainTaskList != null) {
                for (String mainTask : mainTaskList) {
                    //删除所有的子任务
                    clearSubTask(mainTask);
                    //剔除主任务
                    rdsMainTask.remove(mainTaskName, mainTask);
                }
            }
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } while (mainTaskList != null && !mainTaskList.isEmpty());
        //清理执行队列
        clearExecuteQueue();
        //删除所有主任务
        clearMainTask();
        return true;
    }

    /**
     * 清除主任务下的所有子任务
     *
     * @param mainTask
     * @return
     */
    public boolean clearSubTask(String mainTask) {
        rdsSubTask.clear(mainTask);
        return true;
    }


    /**
     * 清除执行队列
     *
     * @return
     */
    public boolean clearExecuteQueue() {
        executeQueue.clear();
        return true;
    }


    /**
     * 任务完成
     *
     * @param mainTask
     * @param subTask
     * @return
     */
    public boolean finishSubTask(String mainTask, String subTask) {
        //删除子任务
        rdsSubTask.remove(mainTask, subTask);
        //删除限流
        executeQueue.remove(subTask);
        return true;
    }

    /**
     * 判断所有的子任务是否都已经完成
     *
     * @param mainTask
     * @return
     */
    @Override
    public boolean isAllSubTaskFinished(String mainTask) {
        //Set<String> result = rdsSubTask.poll(mainTask, 0, -1, 0, 1);
        //result==null||result.size()==0;
        return rdsSubTask.size(mainTask) == 0;
    }

    /**
     * 清理子任务执行队列
     */
    public void clearLimitQueue() {
        executeQueue.clear();
    }


}
