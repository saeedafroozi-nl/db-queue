package com.bestseller.dbqueue.core.config.impl;

import com.bestseller.dbqueue.core.config.TaskLifecycleListener;

import javax.annotation.Nonnull;

/**
 * Empty listener for task processing lifecycle.
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
public final class NoopTaskLifecycleListener implements TaskLifecycleListener {

    private static final NoopTaskLifecycleListener INSTANCE = new NoopTaskLifecycleListener();

    @Nonnull
    public static NoopTaskLifecycleListener getInstance() {
        return INSTANCE;
    }

}
