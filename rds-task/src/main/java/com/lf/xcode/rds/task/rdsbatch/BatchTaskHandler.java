package com.lf.xcode.rds.task.rdsbatch;


import java.util.Collection;

public interface BatchTaskHandler<M, S> {
    boolean addSubTask(M m, S s);

    boolean addSubTask(M m, Collection<S> s);

    boolean addMainTask(M m);

    boolean mainTaskHasFree();

    int mainTaskFreeSize();

    boolean clearMainTask();

    boolean clearSubTask(M m);

    boolean clearExecuteQueue();

    boolean finishSubTask(M m, S s);

    boolean isAllSubTaskFinished(M m);

    boolean doBizTask(M m, S s);

    boolean mainLoop();
}
