package com.bestseller.dbqueue.core.api.impl;

import org.junit.Test;
import com.bestseller.dbqueue.core.api.EnqueueParams;
import com.bestseller.dbqueue.core.config.QueueShard;
import com.bestseller.dbqueue.core.config.QueueShardId;
import com.bestseller.dbqueue.core.stub.StubDatabaseAccessLayer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class SingleQueueShardRouterTest {

    @Test
    public void should_return_single_shard() {
        QueueShard<StubDatabaseAccessLayer> main = new QueueShard<>(new QueueShardId("main"),
                new StubDatabaseAccessLayer());
        SingleQueueShardRouter<String, StubDatabaseAccessLayer> router = new SingleQueueShardRouter<>(main);
        assertThat(router.resolveShard(EnqueueParams.create("1")), equalTo(main));
    }
}