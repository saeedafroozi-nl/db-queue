package com.bestseller.dbqueue.core.stub;

import com.bestseller.dbqueue.core.api.QueueConsumer;
import com.bestseller.dbqueue.core.api.TaskPayloadTransformer;
import com.bestseller.dbqueue.core.api.impl.NoopPayloadTransformer;
import com.bestseller.dbqueue.core.settings.QueueConfig;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Queue consumer without payload transformation
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
public abstract class StringQueueConsumer implements QueueConsumer<String> {

    @Nonnull
    private final QueueConfig queueConfig;

    public StringQueueConsumer(@Nonnull QueueConfig queueConfig) {
        this.queueConfig = requireNonNull(queueConfig);
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<String> getPayloadTransformer() {
        return NoopPayloadTransformer.getInstance();
    }

}
