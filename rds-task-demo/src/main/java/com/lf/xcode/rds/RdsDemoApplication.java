package com.lf.xcode.rds;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableAspectJAutoProxy(exposeProxy = true)
@EnableConfigurationProperties
@EnableScheduling
public class RdsDemoApplication extends SpringBootServletInitializer {

    public static void main(String[] args) {
        System.setProperty("spring.cloud.config.import-check.enabled=","false");
        SpringApplication.run(RdsDemoApplication.class, args);
    }

}
