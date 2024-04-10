package com.bestseller.dbqueue.core.config.impl;

import com.bestseller.dbqueue.core.api.TaskExecutionResult;
import com.bestseller.dbqueue.core.api.TaskRecord;
import com.bestseller.dbqueue.core.config.QueueShardId;
import com.bestseller.dbqueue.core.config.TaskLifecycleListener;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * Task listener with logging support
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class LoggingTaskLifecycleListener implements TaskLifecycleListener {

    private static final Logger log = LoggerFactory.getLogger(LoggingTaskLifecycleListener.class);

    private final Clock clock;

    /**
     * Constructor
     */
    public LoggingTaskLifecycleListener() {
        this(Clock.systemDefaultZone());
    }

    /**
     * Constructor for testing purpose
     *
     * @param clock A clock to mock current time
     */
    LoggingTaskLifecycleListener(@Nonnull Clock clock) {
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                        @Nonnull TaskRecord taskRecord) {
        log.info("consuming task: id={}, attempt={}", taskRecord.getId(), taskRecord.getAttemptsCount());
    }

    @Override
    public void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                         @Nonnull TaskRecord taskRecord,
                         @Nonnull TaskExecutionResult executionResult, long processTaskTime) {
        switch (executionResult.getActionType()) {
            case FINISH -> {
                Duration inQueueTime = Duration.between(taskRecord.getCreatedAt(), ZonedDateTime.now(clock));
                log.info("task finished: id={}, in-queue={}, time={}", taskRecord.getId(), inQueueTime, processTaskTime);
            }
            case REENQUEUE -> log.info("task reenqueued: id={}, delay={}, time={}", taskRecord.getId(),
                    executionResult.getExecutionDelay().orElse(null), processTaskTime);
            case FAIL -> log.info("task failed: id={}, time={}", taskRecord.getId(), processTaskTime);
            default -> log.warn("unknown action type: type={}", executionResult.getActionType());
        }
    }

    @Override
    public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                        @Nonnull TaskRecord taskRecord,
                        @Nullable Exception exc) {
        log.error("error while processing task: task={}", taskRecord, exc);
    }

}
