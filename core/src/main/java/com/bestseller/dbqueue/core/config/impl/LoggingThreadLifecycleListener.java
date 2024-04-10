package com.bestseller.dbqueue.core.config.impl;

import com.bestseller.dbqueue.core.config.QueueShardId;
import com.bestseller.dbqueue.core.config.ThreadLifecycleListener;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Thread listener with logging support
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class LoggingThreadLifecycleListener implements ThreadLifecycleListener {

    private static final Logger log = LoggerFactory.getLogger(LoggingThreadLifecycleListener.class);

    @Override
    public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                        @Nullable Throwable exc) {
        log.error("fatal error in queue thread: shardId={}, location={}", shardId.asString(),
                location, exc);
    }
}
