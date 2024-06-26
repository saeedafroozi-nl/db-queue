package com.bestseller.dbqueue.core.api.impl;

import com.bestseller.dbqueue.core.api.EnqueueParams;
import com.bestseller.dbqueue.core.api.QueueShardRouter;
import com.bestseller.dbqueue.core.config.DatabaseAccessLayer;
import com.bestseller.dbqueue.core.config.QueueShard;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Shard router without sharding. Might be helpful if you have single database instance.
 *
 * @param <PayloadT>             The type of the payload in the task
 * @param <DatabaseAccessLayerT> The type of the database access layer
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class SingleQueueShardRouter<PayloadT, DatabaseAccessLayerT extends DatabaseAccessLayer>
        implements QueueShardRouter<PayloadT, DatabaseAccessLayerT> {

    @Nonnull
    private final QueueShard<DatabaseAccessLayerT> queueShard;

    /**
     * Constructor
     *
     * @param queueShard queue shard
     */
    public SingleQueueShardRouter(@Nonnull QueueShard<DatabaseAccessLayerT> queueShard) {
        this.queueShard = Objects.requireNonNull(queueShard, "queueShard must not be null");
    }

    @Override
    public QueueShard<DatabaseAccessLayerT> resolveShard(EnqueueParams<PayloadT> enqueueParams) {
        return queueShard;
    }
}
