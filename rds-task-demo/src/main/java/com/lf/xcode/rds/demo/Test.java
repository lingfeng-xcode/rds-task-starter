package com.lf.xcode.rds.demo;

import cn.hutool.core.util.RandomUtil;
import com.lf.xcode.rds.demo.DemoTaskHandler;
import com.lf.xcode.rds.task.rdsbatch.RdsBatchTaskHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试工具
 */
@Slf4j
@Service
public class Test {

    private final DemoTaskHandler demoTaskHandler;
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final AtomicInteger finishCount = new AtomicInteger(0);
    private final ConcurrentHashMap<String, String> result = new ConcurrentHashMap<>();


    public Test(DemoTaskHandler demoTaskHandler) {
        this.demoTaskHandler = demoTaskHandler;
        //注册业务处理器
        demoTaskHandler.registerBizInvoke(param -> this.executeSubTask(param.getMainTask(), param.getSubTask()));
    }

    /**
     * 创建任务
     * @return
     */
    private Map<String, List<String>> generateMyTask() {
        int mediaCount = 15;
        int subMediaCount = 30;
        Map<String, List<String>> map = new HashMap<>();
        for (int i = 0; i < mediaCount; i++) {
            String mediaId = "mediaId_" + i + "_" + RandomUtil.randomString(5);
            List<String> subMediaIds = new ArrayList<>(subMediaCount);
            for (int j = 0; j < subMediaCount; j++) {
                subMediaIds.add("subMediaId_" + j + "_" + RandomUtil.randomString(5));
            }
            map.put(mediaId, subMediaIds);
        }
        log.info("map={}", map.size());
        return map;
    }

    @PostConstruct
    public void init() {
        //清除之前的所有任务
        demoTaskHandler.clearAll();
        //生成任务集合
        Map<String, List<String>> allTask = generateMyTask();

        //添加任务
        new Thread(() -> {
            Iterator<Map.Entry<String, List<String>>> iterator = allTask.entrySet().iterator();
            while (!allTask.isEmpty()) {
                log.info("开始扫描任务");
                //主任务是否达到阈值
                if (demoTaskHandler.getMainTaskFreeSize() > 0) {
                    while (iterator.hasNext()) {
                        if ((demoTaskHandler.getMainTaskFreeSize() > 0)) {
                            Map.Entry<String, List<String>> next = iterator.next();
                            String mediaId = next.getKey();
                            List<String> batchSubMediaIds = next.getValue();
                            //添加子任务，添加完毕后由 DemoTaskHandler.schedule 进行调度
                            demoTaskHandler.addSubTask(mediaId, batchSubMediaIds);
                            //绑定主任务
                            demoTaskHandler.addMainTask(mediaId);
                            //删除已经添加的任务
                            iterator.remove();
                        } else {
                            break;
                        }
                    }
                }
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    }

    /**
     * 子任务的执行
     *
     * @param mainTask
     * @param subTask
     * @return
     */
    public boolean executeSubTask(String mainTask, String subTask) {
        RdsBatchTaskHandler rdsBatchTaskHandler = demoTaskHandler.getRdsBatchTaskHandler();
        executor.execute(() -> {
            try {
                TimeUnit.SECONDS.sleep(RandomUtil.randomInt(1, 2));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                log.info("executeSubTask任务完成 finishCount={} mainTask:{} subTask:{}", finishCount.incrementAndGet(), mainTask, subTask);
            }
            //进行回调通知任务已经完成
            rdsBatchTaskHandler.finishSubTask(mainTask, subTask);
            String exit = result.putIfAbsent(mainTask + "_" + subTask, "1");
            if (exit != null) {
                log.info("重复执行任务 mainTask:{} subTask:{} exit:{}", mainTask, subTask, exit);
            }
        });
        return true;
    }

}
