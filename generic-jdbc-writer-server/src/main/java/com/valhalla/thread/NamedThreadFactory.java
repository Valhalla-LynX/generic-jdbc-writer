package com.valhalla.thread;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author : LynX
 * @create: 2021-11-18 13:20
 **/
public class NamedThreadFactory implements ThreadFactory {
    public final String namePrefix;
    private final ThreadGroup threadGroup;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public NamedThreadFactory(String name) {
        threadGroup = Thread.currentThread().getThreadGroup();
        if (null == name || name.trim().isEmpty()) {
            name = "pool";
        }
        AtomicInteger poolNumber = new AtomicInteger(1);
        namePrefix = name + "-" +
                poolNumber.getAndIncrement() +
                "-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(threadGroup, r,
                namePrefix + threadNumber.getAndIncrement(),
                0);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }
}
