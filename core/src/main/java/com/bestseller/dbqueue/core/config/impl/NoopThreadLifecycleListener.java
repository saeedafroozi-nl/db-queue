package com.bestseller.dbqueue.core.config.impl;

import com.bestseller.dbqueue.core.config.ThreadLifecycleListener;

import javax.annotation.Nonnull;

/**
 * Empty listener for task processing thread in the queue.
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
public class NoopThreadLifecycleListener implements ThreadLifecycleListener {

    private static final NoopThreadLifecycleListener INSTANCE = new NoopThreadLifecycleListener();

    @Nonnull
    public static NoopThreadLifecycleListener getInstance() {
        return INSTANCE;
    }

}
