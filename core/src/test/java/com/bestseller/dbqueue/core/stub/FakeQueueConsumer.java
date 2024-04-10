package com.bestseller.dbqueue.core.stub;

import com.bestseller.dbqueue.core.api.QueueConsumer;
import com.bestseller.dbqueue.core.api.Task;
import com.bestseller.dbqueue.core.api.TaskExecutionResult;
import com.bestseller.dbqueue.core.api.TaskPayloadTransformer;
import com.bestseller.dbqueue.core.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class FakeQueueConsumer implements QueueConsumer<String> {

    private final QueueConfig queueConfig;
    private final TaskPayloadTransformer<String> transformer;
    private final Function<Task<String>, TaskExecutionResult> execFunc;

    public FakeQueueConsumer(QueueConfig queueConfig, TaskPayloadTransformer<String> transformer,
                             Function<Task<String>, TaskExecutionResult> execFunc) {
        this.queueConfig = queueConfig;
        this.transformer = transformer;
        this.execFunc = execFunc;
    }

    @Nonnull
    @Override
    public TaskExecutionResult execute(@Nonnull Task<String> task) {
        return execFunc.apply(task);
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<String> getPayloadTransformer() {
        return transformer;
    }

}
