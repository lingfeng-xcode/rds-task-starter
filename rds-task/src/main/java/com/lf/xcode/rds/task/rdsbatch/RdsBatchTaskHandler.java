package com.lf.xcode.rds.task.rdsbatch;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.RandomUtil;
import com.lf.xcode.rds.task.rdsbatch.config.BatchTaskConfig;
import com.lf.xcode.rds.task.rdsbatch.param.InvokeParam;
import com.lf.xcode.rds.util.ExceptionUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 基于redis的批量任务处理器
 */
@Getter
@Slf4j
public class RdsBatchTaskHandler extends AbstractBatchTaskHandler<String> {


    /**
     * @param jedisSupplier   获取jedis的方法
     * @param distributedLock 分布式锁
     * @param config          任务配置
     * @param bizFunction     业务处理器
     * @param mainKey         主任务的key
     * @param prefix          任务的前缀
     */
    public RdsBatchTaskHandler(Supplier<Jedis> jedisSupplier,
                               Function<Supplier<Boolean>, Boolean> distributedLock,
                               BatchTaskConfig config,
                               Function<InvokeParam, Boolean> bizFunction,
                               String mainKey, String prefix) {
        super(jedisSupplier, distributedLock, config, bizFunction, mainKey, prefix);
        log.info("{}{}RdsBatchTaskHandler init", mainKey, prefix);
    }


    /**
     * 循环执行任务
     *
     * @return
     */
    @Override
    public boolean mainLoop() {
        long start = System.currentTimeMillis();
        try {
            //前置处理
            preHandle();
            //任务处理
            mainHandle();
            //后置处理
            postHandle();
        } finally {
            long end = System.currentTimeMillis();
            //如果本次循环时间小于分布式锁等待超时时间，则休眠一段时间,强制让其他等锁的线程超时，避免再次执行
            long diff = config.getMainLoopLockTimeout() - (end - start);
            //补充睡眠
            if (diff > 0) {
                try {
                    diff = diff + RandomUtil.randomInt(500, 1000);
                    log.info("mainLoop_sleep {}", diff);
                    TimeUnit.MILLISECONDS.sleep(diff);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return true;
    }


    /**
     * 执行任务
     *
     * @return
     */
    @Override
    public boolean doBizTask(String mainTask, String subTask) {
        try {
            return bizHandler.apply(new InvokeParam().setMainTask(mainTask).setSubTask(subTask));
        } catch (Exception e) {
            log.info(e.getMessage(), ExceptionUtil.getMessage(e, 10));
            return false;
        }
    }

    @Override
    protected void preHandle() {
        //清理过期任务
        clearOutTimeTask(config.getMainLoopSize());
    }

    /**
     * 主处理器
     * 1.按照优先级获取所有的主任务
     * 2.获取每个主任务的部分等待执行的子任务
     * 3.根据执行队列空闲程度，执行各个主任务的子任务
     * 4.子任务执行采用非公平锁进行抢队执行
     */
    @Override
    protected void mainHandle() {
        int pageNum = 0;
        //每次批量查询10个主任务
        List<String> mainTaskList = null;
        log.info("mainHandler_begin");
        int mainLoopSize = config.getMainLoopSize();
        AtomicInteger running = new AtomicInteger(0);
        do {
            //数字越小优先级越高
            Set<String> dbList = rdsMainTask.pollAsc(mainTaskName, DEFAULT_PRIORITY_HIGH, DEFAULT_PRIORITY, pageNum, mainLoopSize);
            if (dbList != null && !dbList.isEmpty()) {
                log.info("mainHandle_mainTaskList size {} list {}", dbList.size(), dbList);
                mainTaskList = new ArrayList<>(dbList);
                if (AllocationPolicy.RANDOM == config.getAllocationPolicy()) {
                    //随机打乱list, 这个没有作用了，下面的代码按劳分配了
                    Collections.shuffle(mainTaskList);
                }
                //剩余任务数量
                int remain = executeQueue.freeSize();
                log.info("executeQueue_remain {}", remain);
                //等待执行的任务数量
                Map<String, Integer> waitMap = new HashMap<>();
                //等待任务的总数量
                AtomicInteger wait = new AtomicInteger(0);
                mainTaskList.forEach(mainTask -> {
                    //主任务下待执行的子任务数量
                    int waitCount = rdsSubTask.size(mainTask, SUB_TASK_WAIT, SUB_TASK_WAIT);
                    waitMap.put(mainTask, waitCount);
                    wait.addAndGet(waitCount);
                });
                //主任务使用非公平锁
                waitMap.entrySet().stream().parallel().forEach(item -> {
                    String mainTask = item.getKey();
                    //等待的任务数/总的等待数量 = 比例
                    float rate = waitMap.get(mainTask).floatValue() / wait.get();
                    //根据比例计算每个主任务的执行数量 每个主任务分配的可执行数量 至少为1，尽可能饱和
                    int count = ((int) (remain * rate)) + 1;
                    log.info("assign_job main {} count {}", mainTask, count);
                    if (executeQueue.free()) {
                        //分批获取 subTaskLimit个 子任务
                        Set<String> subTaskList = rdsSubTask.poll(mainTask, SUB_TASK_WAIT, 0, count);
                        log.info("subTaskList size= {} list = {}", subTaskList.size(), subTaskList);
                        //子任务在执行完毕后，会降低limit，这样就可以继续分配了
                        if (CollUtil.isNotEmpty(subTaskList)) {
                            for (String subTask : subTaskList) {
                                //删除子任务的情景: 1.任务执行失败 2.异步回调，调用任务完成接口 3.任务超时
                                //executeQueue.contains(subTask) 的含义是支持父任务重复执行，但子任务不可重复执行
                                if (!executeQueue.contains(subTask) &&
                                        //尝试进入执行队列
                                        executeQueue.add(subTask)) {
                                    //是否使用线程池执行业务，此处不必担心，线程池队列满了,因为executeQueue.add会对线程池进行限制
                                    if (config.isUseThreadPool()) {
                                        //采用异步线程池执行
                                        bizThreadPool.execute(() -> runBizWork(mainTask, subTask, running));
                                    } else {
                                        //parallel线程直接运行
                                        runBizWork(mainTask, subTask, running);
                                    }
                                }
                            }
                        } else {
                            //说明所有的子任务都已经在执行了，主任务不在进入循环，删除主任务
                            delMainTask(mainTask);
                        }
                    }
                });
            }
            //如果任务队列已经满了，则不再继续分配
            if (!executeQueue.free()) {
                break;
            }
            pageNum++;
        } while (mainTaskList != null && mainTaskList.size() >= mainLoopSize);
        log.info("本次循环分配了 {} 个任务", running.get());
    }


    /**
     * 执行子任务
     * 1.如果执行成功，更新子任务为执行中状态，这样下次扫描不会再次分配
     * 2.如果执行失败，不更改子任务状态，将任务从执行队列中剔除，增加子任务重试次数
     * 3.如果重试次数超过最大重试次数，则删除子任务，再也不会执行该任务
     *
     * @param mainTask
     * @param subTask
     * @param running
     */

    private void runBizWork(String mainTask, String subTask, final AtomicInteger running) {
        //执行业务逻辑
        if (doBizTask(mainTask, subTask)) {
            //更新子任务为执行中状态，这样下次扫描不会再次分配
            rdsSubTask.set(mainTask, subTask, SUB_TASK_RUNNING);
            //更新主任务为高优先级，优先集中处理同一个主任务的子任务
            upgradeMainTaskPriority(mainTask);
            //执行成功，计数器+1
            running.incrementAndGet();
        } else {
            //业务逻辑执行失败，说明需要重试
            //将子任务从执行队列中剔除
            executeQueue.remove(subTask);
            log.info("subTask biz_run_failed {}", subTask);
            //增加重试次数
            retryRecorder.addRetryRecord(mainTask, subTask);
            if (retryRecorder.getRetryCount(mainTask, subTask) > config.getRetryCount()) {
                //删除重试记录
                retryRecorder.delRetryRecord(mainTask, subTask);
                //删除子任务，再也不会执行该任务
                log.info("subTask did_max_retryCount_drop_task {}", subTask);
                rdsSubTask.remove(mainTask, subTask);
            }
        }
    }

    /**
     * 删除过期任务
     *
     * @param mainLoopSize
     */
    private void clearOutTimeTask(int mainLoopSize) {
        //获取执行过期的任务
        long deadline = System.currentTimeMillis() - config.getTaskTimeout();
        Set<String> expireTaskList = executeQueue.poll(mainLoopSize, deadline);
        if (!expireTaskList.isEmpty()) {
            //删除过期任务
            expireTaskList.forEach(task -> {
                long time = executeQueue.getTaskExecuteTime(task);
                log.info("expireTaskList task {} time {}", task, time);
                executeQueue.remove(task);
            });
        }
    }

    /**
     * 后置处理
     */
    @Override
    protected void postHandle() {
        log.info("postHandler开始处理");
        checkAndClearMainTask();
    }

    /**
     * 处理所有已经完成的任务
     * 如果所有子任务都已经完成，则删除主任务
     */
    private void checkAndClearMainTask() {
        //获取所有已经完成的任务
        int pageNum = 0;
        int pageSize = 10;
        //只查询高优先级的任务
        Set<String> mainTaskList = rdsMainTask.pollAsc(mainTaskName, DEFAULT_PRIORITY_HIGH, DEFAULT_PRIORITY_HIGH, pageNum, pageSize);
        if (mainTaskList != null) {
            for (String mainTask : mainTaskList) {
                // 检测等待执行的子任务 limit 1
                Set<String> subTask = rdsSubTask.poll(mainTask, SUB_TASK_WAIT, 0, 1);
                //说明所有的子任务都已经在执行了，主任务不在进入循环，删除主任务
                if (subTask == null || subTask.size() == 0) {
                    log.info("检测出主任务已经完成删除主任务:{}", mainTask);
                    delMainTask(mainTask);
                }
            }
        }
    }
}
