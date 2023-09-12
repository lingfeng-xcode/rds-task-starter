package com.lf.xcode.rds.task.rdstask;

import lombok.Setter;
import redis.clients.jedis.Jedis;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.function.Supplier;

import static com.lf.xcode.rds.task.constant.NameSpaceConstant.NAMESPACE;

/**
 * 基于redis实现的zset任务
 * 1.limit 非线程安全 不支持自动限制
 * 2.支持排序查询
 */
public class RdsZsetTask implements ZsetTask<String> {

    //任务的前缀
    public static final String XCODE_RDS_TASK_RDS_ZSET_TASK = NAMESPACE + ":ZSET_TASK:";
    //默认超时时间: 1个月
    private static final long DEFAULT_TIMEOUT_MS = 1000 * 60 * 60 * 24 * 30L;
    private final Supplier<Jedis> jedisSupplier;
    private final String taskType;
    //默认任务limit
    private final Supplier<Integer> limitSupplier;
    @Setter
    //默认过期时间
    private final Supplier<Long> expireFunc;

    public static final int NO_LIMIT = -1;
    public static final long NO_EXPIRE = -1L;

    public RdsZsetTask(String taskType, Supplier<Jedis> jedisSupplier, Supplier<Integer> limitSupplier, Supplier<Long> expireFunc) {
        this.taskType = taskType.endsWith(":") ? taskType : taskType + ":";
        this.jedisSupplier = jedisSupplier;
        this.expireFunc = expireFunc;
        this.limitSupplier = limitSupplier;
    }


    private String generateKey(String key) {
        return XCODE_RDS_TASK_RDS_ZSET_TASK + taskType + key;
    }

    /**
     * 计算任务的过期时间
     *
     * @param key
     * @return
     */
    private long getTaskTimeoutMsAt(String key) {
        //每个任务的超时时间总和即为，总任务的存活时间
        if (expireFunc.get() == null || expireFunc.get() == NO_EXPIRE) {
            return NO_EXPIRE;
        }
        long timeout = expireFunc.get() == 0 ? DEFAULT_TIMEOUT_MS : expireFunc.get() * (size(key) + 1);
        long ttl = ttl(key); //获取当前的过期时间
        long newTTL = System.currentTimeMillis() + timeout;
        return ttl > newTTL ? ttl + timeout : newTTL;
    }

    /**
     * 获取当前的过期时间
     *
     * @param key
     * @return
     */
    private long ttl(String key) {
        try (Jedis jedis = jedisSupplier.get()) {
            Long ttl = jedis.ttl(generateKey(key));
            return ttl == null ? 0 : ttl;
        }
    }

    @Override
    public boolean add(String key, String value, double score) {
        try (Jedis jedis = jedisSupplier.get()) {
            jedis.zadd(generateKey(key), score, value);
            setExpire(jedis, generateKey(key));
        }
        return true;
    }

    /**
     * 设置过期时间
     *
     * @param jedis
     * @param key
     */
    private void setExpire(Jedis jedis, String key) {
        long expireTime = getTaskTimeoutMsAt(key);
        if (expireTime != NO_EXPIRE) {
            jedis.pexpireAt(key, expireTime);
        } else {
            //不设置过期时间
        }
    }

    @Override
    public boolean add(String key, Collection<String> value, double score) {
        HashMap<String, Double> map = new HashMap<>(value.size());
        for (String s : value) {
            map.put(s, score);
        }
        try (Jedis jedis = jedisSupplier.get()) {
            jedis.zadd(generateKey(key), map);
            setExpire(jedis, generateKey(key));
        }
        return true;
    }

    @Override
    public boolean set(String key, String value, double score) {
        return add(key, value, score);
    }

    @Override
    public Set<String> pollAsc(String key, int pageNum, int pageSize) {
        return pollAsc(key, 0, -1, pageNum, pageSize);
    }

    @Override
    public Set<String> pollAsc(String key, double min, double max, int pageNum, int pageSize) {
        pageSize = pageSize < 0 ? 100 : pageSize;
        pageNum = pageNum <= 0 ? 1 : pageNum;
        int offset = (pageNum - 1) * pageSize;
        try (Jedis jedis = jedisSupplier.get()) {
            return jedis.zrangeByScore(generateKey(key), min, max, offset, pageSize);
        }
    }

    @Override
    public Set<String> pollDesc(String key, double max, double min, int pageNum, int pageSize) {
        pageSize = pageSize < 0 ? 10 : pageSize;
        pageNum = pageNum <= 0 ? 1 : pageNum;
        int offset = (pageNum - 1) * pageSize;
        try (Jedis jedis = jedisSupplier.get()) {
            return jedis.zrevrangeByScore(generateKey(key), max, min, offset, pageSize);
        }
    }

    @Override
    public Set<String> pollDesc(String key, int pageNum, int pageSize) {
        return pollDesc(key, -1, 0, pageNum, pageSize);
    }

    @Override
    public Set<String> poll(String key, double value, int pageNum, int pageSize) {
        pageSize = pageSize < 0 ? 10 : pageSize;
        pageNum = pageNum <= 0 ? 1 : pageNum;
        int offset = (pageNum - 1) * pageSize;
        try (Jedis jedis = jedisSupplier.get()) {
            return jedis.zrangeByScore(generateKey(key), value, value, offset, pageSize);
        }
    }

    @Override
    public Set<String> poll(String key, double min, double max, int pageNum, int pageSize) {
        pageSize = pageSize < 0 ? 10 : pageSize;
        pageNum = pageNum <= 0 ? 1 : pageNum;
        int offset = (pageNum - 1) * pageSize;
        try (Jedis jedis = jedisSupplier.get()) {
            return jedis.zrangeByScore(generateKey(key), min, max, offset, pageSize);
        }
    }

    @Override
    public boolean remove(String key, String value) {
        try (Jedis jedis = jedisSupplier.get()) {
            return jedis.zrem(generateKey(key), value) == 1;
        }
    }

    @Override
    public int size(String key) {
        try (Jedis jedis = jedisSupplier.get()) {
            Long zcard = jedis.zcard(generateKey(key));
            return zcard == null ? 0 : zcard.intValue();
        }
    }

    @Override
    public int size(String key, double min, double max) {
        try (Jedis jedis = jedisSupplier.get()) {
            Long zcount = jedis.zcount(generateKey(key), min + "", max + "");
            return zcount == null ? 0 : zcount.intValue();
        }
    }

    @Override
    public int freeSize(String key) {
        return limitSupplier.get() - size(key);
    }

    @Override
    public void clear(String key) {
        try (Jedis jedis = jedisSupplier.get()) {
            jedis.del(generateKey(key));
        }
    }
}
