package com.lf.xcode.rds.demo.base;

import groovy.lang.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.integration.redis.util.RedisLockRegistry;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

@Slf4j
public class BaseUtil {


    //分布式锁的超时时间
    private static final long default_lock_timeout = 10 * 60 * 1000L;


    public static <T> T safeOnePodDo(RedisLockRegistry lockRegistry, String key, Supplier<T> action, long timeout) {
        return safeOnePodDo(lockRegistry, key, action, "", timeout);
    }

    public static <T> T safeOnePodDo(RedisLockRegistry lockRegistry, String key, Supplier<T> action, String lockFailedMsg) {
        return safeOnePodDo(lockRegistry, key, action, lockFailedMsg, default_lock_timeout);
    }

    public static <T> T safeOnePodDo(RedisLockRegistry lockRegistry, String key, Supplier<T> action) {
        return safeOnePodDo(lockRegistry, key, action, "", default_lock_timeout);
    }


    /**
     * 分布式锁
     *
     * @param lockRegistry
     * @param key
     * @param action
     * @param lockFailedMsg
     * @param timeout
     * @return
     */
    public static <T> T safeOnePodDo(RedisLockRegistry lockRegistry, String key, Supplier<T> action, String lockFailedMsg, long timeout) {
        try {
            Lock obtain = lockRegistry.obtain(key);
            if (obtain.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                try {
                    return action.get();
                } finally {
                    obtain.unlock();
                }
            } else {
                log.error("safeOnePodDo get lock failed msg={} {}", lockFailedMsg, key);
            }
        } catch (InterruptedException e) {
            log.error(e.getMessage(), ExceptionUtils.getMessage(e));
        }
        //决定是否重试
        return null;
    }

    /**
     * 对集合进行分组
     *
     * @param list
     * @param groupCount
     * @param <T>
     * @return
     */
    public static <T> Map<Integer, List<T>> groupCollection(Collection<T> list, int groupCount) {
        Map<Integer, List<T>> group = new HashMap<>();
        Iterator<T> iterator = list.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            T next = iterator.next();
            int j = i % (groupCount);
            {
                if (!group.containsKey(j)) {
                    group.put(j, new ArrayList<>(list.size() / groupCount));
                }
                List<T> sub = group.get(j);
                sub.add(next);
            }
            i++;
        }
        return group;
    }


    /**
     * 数据分段点
     * 将min-max范围内数据 按照percent百分比 分成相等的数据段
     *
     * @param min
     * @param max
     * @param percent
     * @return
     */
    public static List<Tuple2<Long, Long>> segmentPoint(long min, long max, float percent) {
        //数据分段:在最大和最小的id中进行数据分段, 分段总数和=数据总数
        //获取总长度
        long total = max - min;
        //真正需要的总数
        long realSize = new Float((total * percent)).longValue();
        //需要多少段
        long segment = total / realSize;
        //不能整除则+1段
        if (total % realSize != 0) {
            segment += 1;
        }
        //每段长度
        long staff = total / segment;
        //断点
        List<Tuple2<Long, Long>> segmentPoints = new ArrayList<>();
        for (long i = min; i < max; i += staff) {
            segmentPoints.add(new Tuple2<>(i, i + staff));
        }
        return segmentPoints;
    }

    public static boolean arrayEquals(String arr1, String arr2) {
        if (Objects.equals(arr1, arr2)) {
            return true;
        }
        String[] split1 = arr1.split(",");
        String[] split2 = arr2.split(",");
        Arrays.sort(split1);
        Arrays.sort(split2);
        return Arrays.equals(split1, split2);
    }
}
