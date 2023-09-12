package com.lf.xcode.rds.task.rdstask;

import java.util.Collection;
import java.util.Set;

public interface ZsetTask<T> {

    boolean add(String key, T value, double score);

    boolean add(String key, Collection<T> t, double score);

    boolean set(String key, T value, double score);

    Set<T> pollAsc(String key, int pageNum, int pageSize);

    Set<T> pollAsc(String key,double min, double max, int pageNum, int pageSize);

    Set<T> pollDesc(String key, int pageNum, int pageSize);
    Set<T> pollDesc(String key, double max, double min, int pageNum, int pageSize);

    Set<T> poll(String key, double score, int pageNum, int pageSize);

    Set<T> poll(String key, double min, double max, int pageNum, int pageSize);

    boolean remove(String key, T t);

    int size(String key);

    int size(String key, double min, double max);

    int freeSize(String key);
    void clear(String key);

}
