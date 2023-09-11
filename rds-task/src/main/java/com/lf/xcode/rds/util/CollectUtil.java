package com.lf.xcode.rds.util;

import java.util.*;

public class CollectUtil {

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
}
