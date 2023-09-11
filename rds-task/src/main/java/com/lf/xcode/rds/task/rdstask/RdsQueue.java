package com.lf.xcode.rds.task.rdstask;

import lombok.Getter;
import lombok.Setter;
import redis.clients.jedis.Jedis;

import java.util.AbstractQueue;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.lf.xcode.rds.task.constant.NameSpaceConstant.NAMESPACE;

/**
 * 基于redis-zset实现的queue队列
 * 结构 key:key,executeTime
 * 1.添加任务线程安全，如果超过限制数量，返回false
 * 2.获取任务支持，根据执行时间返回
 */
public class RdsQueue extends AbstractQueue<String> {

    //全局子任务数量限制
    private static final String XCODE_RDS_TASK_REDIS_QUEUE = NAMESPACE + ":QUEUE:";
    public final static long DEFAULT_TIMEOUT_MS = 1000 * 60 * 60 * 24 * 30 * 3;

    private Supplier<Jedis> jedisSupplier;
    //分布式锁
    private Function<Supplier<Boolean>, Boolean> distributedLock;
    private String name;
    private Supplier<Integer> limit;
    @Getter
    @Setter
    private Supplier<Long> expireFunc;

    public RdsQueue(String name, Supplier<Jedis> jedisSupplier, Function<Supplier<Boolean>, Boolean> distributedLock,
                    Supplier<Integer> limit, Supplier<Long> expireFunc) {
        this.distributedLock = distributedLock;
        this.jedisSupplier = jedisSupplier;
        this.limit = limit;
        this.name = name;
        this.expireFunc = expireFunc;
    }

    private String getQueueName() {
        return XCODE_RDS_TASK_REDIS_QUEUE + name;
    }

    /**
     * 每次任务添加成功，会更新任务超时时间
     *
     * @return
     */
    private long queueExpireAtTime() {
        long perTaskTimeout = expireFunc.get() == null ? DEFAULT_TIMEOUT_MS : expireFunc.get();
        //两倍的超时时间
        long expireAtTime = System.currentTimeMillis() + perTaskTimeout * 2;
        //获取当前的过期时间
        long expire = expire();
        //如果过期时间比计算出来的新的时间还要大，则使用过期时间+超时时间
        return expire > expireAtTime ? expire + perTaskTimeout : expireAtTime;
    }

    /**
     * 获取过期时间
     *
     * @return
     */
    private long expire() {
        try (Jedis jedis = jedisSupplier.get()) {
            Long pttl = jedis.pttl(getQueueName());
            return pttl == null ? 0 : pttl;
        }
    }

    @Override
    @Deprecated
    public Iterator<String> iterator() {
        return null;
    }

    @Override
    public int size() {
        try (Jedis jedis = jedisSupplier.get()) {
            Long zcard = jedis.zcard(getQueueName());
            return zcard == null ? 0 : zcard.intValue();
        }
    }


    @Override
    public boolean add(String t) {
        return this.offer(t);
    }

    public boolean contains(String t) {
        try (Jedis jedis = jedisSupplier.get()) {
            return jedis.zscore(getQueueName(), t) != null;
        }
    }

    @Override
    public boolean offer(String s) {
        String limitKey = getQueueName();
        Boolean result = distributedLock.apply(() -> {
            long now = System.currentTimeMillis();
            //分布式线程安全
            try (Jedis jedis = jedisSupplier.get()) {
                Long count = jedis.zcard(limitKey);
                if (count == null || count < limit.get()) {
                    //双重锁如果没有改任务添加进来
                    if (jedis.zscore(getQueueName(), s) == null) {
                        //添加子任务
                        jedis.zadd(limitKey, now, s);
                        return true;
                    }
                }
            } finally {
                try (Jedis jedis = jedisSupplier.get()) {
                    //设置队列过期时间
                    jedis.pexpireAt(limitKey, queueExpireAtTime());
                }
            }
            //add失败
            return false;
        });
        return result != null && result;
    }

    @Override
    public String poll() {
        Set<String> set = poll(1);
        return set == null || set.isEmpty() ? null : set.iterator().next();
    }

    public Set<String> poll(int count) {
        return poll(count, -1);
    }

    /**
     * 获取指定数量的过期任务
     *
     * @param count
     * @param deadline
     * @return
     */
    public Set<String> poll(int count, long deadline) {
        try (Jedis jedis = jedisSupplier.get()) {
            String limitKey = getQueueName();
            //从执行队列中获取一个任务，按照执行时间排序
            Set<String> set = jedis.zrangeByScore(limitKey, 0, deadline, 0, count);
            return set == null ? Collections.emptySet() : set;
        }
    }

    /**
     * 获取任务开始执行的时间
     *
     * @param key
     * @return
     */
    public long getTaskExecuteTime(String key) {
        try (Jedis jedis = jedisSupplier.get()) {
            String limitKey = getQueueName();
            Double score = jedis.zscore(limitKey, key);
            return score == null ? -1 : score.longValue();
        }
    }

    @Override
    @Deprecated
    public String peek() {
        return null;
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) return false;
        try (Jedis jedis = jedisSupplier.get()) {
            String limitKey = getQueueName();
            return jedis.zrem(limitKey, o.toString()) == 1;
        }
    }

    @Override
    public void clear() {
        try (Jedis jedis = jedisSupplier.get()) {
            jedis.del(getQueueName());
        }
    }

    /**
     * 空闲大小
     *
     * @return
     */
    public int freeSize() {
        return limit.get() - size();
    }

    /**
     * 是否空闲
     *
     * @return
     */
    public boolean free() {
        return freeSize() > 0;
    }


}
