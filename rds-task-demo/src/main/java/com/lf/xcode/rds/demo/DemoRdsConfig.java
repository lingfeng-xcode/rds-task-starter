package com.lf.xcode.rds.demo;

import com.lf.xcode.rds.task.rdsbatch.config.BatchTaskConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;


/**
 * 存储处理配置
 */
@Slf4j
@Getter
@Setter
@Configuration
public class DemoRdsConfig extends BatchTaskConfig {


    public DemoRdsConfig() {
        updateConfig(null);
    }

    //可以在此动态更新配置
    public void updateConfig(Properties data) {
        this.setMainTaskLimit(10);
        this.setExecuteTaskLimit(20);
        //设置定时任务超时时间
        this.setMainLoopLockTimeout(5 * 1000);
        this.setUseBizThreadPool(true);
        this.setBizThreadPoolSize(20);
        this.setMainLoopSize(5);
        this.setRetryCount(2);
    }


}
