package com.bestseller.dbqueue.core.stub;

import com.bestseller.dbqueue.core.api.Task;
import com.bestseller.dbqueue.core.api.TaskExecutionResult;
import com.bestseller.dbqueue.core.settings.QueueConfig;

import javax.annotation.Nonnull;

/**
 * @author Oleg Kandaurov
 * @since 14.10.2019
 */
public class NoopQueueConsumer extends StringQueueConsumer {
    public NoopQueueConsumer(@Nonnull QueueConfig queueConfig) {
        super(queueConfig);
    }

    @Nonnull
    @Override
    public TaskExecutionResult execute(@Nonnull Task<String> task) {
        return TaskExecutionResult.finish();
    }
}
