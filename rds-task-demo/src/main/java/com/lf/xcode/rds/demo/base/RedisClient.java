package com.lf.xcode.rds.demo.base;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.PostConstruct;

/**
 * redis客户端
 **/
@Slf4j
@Service
public class RedisClient {
    private static final Logger LOG = LoggerFactory.getLogger(RedisClient.class);

    @Value("${spring.redis.host}")
    private String redisHost;

    @Value("${spring.redis.password}")
    private String redisPassword;

    @Value("${spring.redis.port}")
    private String redisPort;

    // 默认缓存时间
    private static final int EXPIRE = 60000;

    private static final String FAIL = "0";

    private RedisClientConfiguration redisClientConfiguration;
    private JedisPool pool;

    @PostConstruct
    public void init() {
//		URI uri = URI.create(redisHost);
        redisClientConfiguration = new RedisClientConfiguration();
        redisClientConfiguration.setHostName(redisHost);
        redisClientConfiguration.setPassword(redisPassword);
        redisClientConfiguration.setPort(Integer.valueOf(redisPort));

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
        jedisPoolConfig.setMaxTotal(this.redisClientConfiguration.getMaxTotal());
        // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        jedisPoolConfig.setMaxIdle(this.redisClientConfiguration.getMaxIdle());
        jedisPoolConfig.setMinIdle(this.redisClientConfiguration.getMinIdle());
        // 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
        jedisPoolConfig.setMaxWaitMillis((long) this.redisClientConfiguration.getMaxWaitMillis());
        jedisPoolConfig
                .setMinEvictableIdleTimeMillis((long) this.redisClientConfiguration.getMinEvictableIdleTimeMillis());
        jedisPoolConfig.setTestOnBorrow(false);
        jedisPoolConfig.setTestOnReturn(false);
        jedisPoolConfig.setTestWhileIdle(true);
        jedisPoolConfig.setBlockWhenExhausted(false);
        this.pool = new JedisPool(jedisPoolConfig, this.redisClientConfiguration.getHostName(),
                this.redisClientConfiguration.getPort(), this.redisClientConfiguration.getTimeout(),
                this.redisClientConfiguration.getPassword(), this.redisClientConfiguration.getDatabase());
        LOG.info("==================RedisClientComponent init done, hostName={}==================",
                redisClientConfiguration.getHostName());
    }

    public Jedis getJedis() {
        Jedis jedis = this.pool.getResource();
        jedis.select(7);
        return jedis;
    }
}
