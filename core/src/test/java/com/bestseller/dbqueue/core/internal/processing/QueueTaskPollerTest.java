package com.bestseller.dbqueue.core.internal.processing;

import org.junit.Test;
import org.mockito.Mockito;
import com.bestseller.dbqueue.core.api.QueueConsumer;
import com.bestseller.dbqueue.core.config.QueueShardId;
import com.bestseller.dbqueue.core.config.ThreadLifecycleListener;
import com.bestseller.dbqueue.core.internal.runner.QueueRunner;
import com.bestseller.dbqueue.core.settings.QueueConfig;
import com.bestseller.dbqueue.core.settings.QueueId;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import com.bestseller.dbqueue.core.stub.FakeMillisTimeProvider;
import com.bestseller.dbqueue.core.stub.TestFixtures;

import java.time.Duration;
import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class QueueTaskPollerTest {

    @Test
    public void should_perform_success_lifecycle() throws Exception {
        QueueLoop queueLoop = spy(new SyncQueueLoop());
        ThreadLifecycleListener listener = mock(ThreadLifecycleListener.class);
        QueueShardId shardId = new QueueShardId("s1");
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        QueueLocation location = QueueLocation.builder().withTableName("table")
                .withQueueId(new QueueId("queue")).build();
        Duration waitDuration = Duration.ofMillis(100L);
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location,
                TestFixtures.createQueueSettings().withPollSettings(
                        TestFixtures.createPollSettings().withBetweenTaskTimeout(Duration.ZERO)
                                .withNoTaskTimeout(waitDuration)
                                .build()).build()));
        QueueRunner queueRunner = mock(QueueRunner.class);
        when(queueRunner.runQueue(queueConsumer)).thenReturn(QueueProcessingStatus.SKIPPED);

        FakeMillisTimeProvider millisTimeProvider = new FakeMillisTimeProvider(Arrays.asList(7L, 11L));

        new QueueTaskPoller(listener, millisTimeProvider).start(queueLoop, shardId, queueConsumer, queueRunner);

        verify(queueLoop).doRun(any());
        verify(listener).started(shardId, location);
        verify(queueRunner).runQueue(queueConsumer);
        verify(listener).executed(shardId, location, false, 4);
        verify(queueLoop).doWait(waitDuration, QueueLoop.WaitInterrupt.ALLOW);
        verify(listener).finished(shardId, location);
    }


    @Test
    public void should_perform_crash_lifecycle() throws Exception {
        QueueLoop queueLoop = spy(new SyncQueueLoop());
        ThreadLifecycleListener listener = mock(ThreadLifecycleListener.class);
        QueueShardId shardId = new QueueShardId("s1");
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        QueueLocation location = QueueLocation.builder().withTableName("table")
                .withQueueId(new QueueId("queue")).build();
        Duration fatalCrashTimeout = Duration.ofDays(1L);
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location,
                TestFixtures.createQueueSettings().withPollSettings(
                        TestFixtures.createPollSettings().withBetweenTaskTimeout(Duration.ZERO)
                                .withNoTaskTimeout(Duration.ZERO)
                                .withFatalCrashTimeout(fatalCrashTimeout)
                                .build()).build()));
        QueueRunner queueRunner = mock(QueueRunner.class);

        RuntimeException exception = new RuntimeException("exc");
        when(queueRunner.runQueue(queueConsumer)).thenThrow(exception);

        new QueueTaskPoller(listener, Mockito.mock(MillisTimeProvider.class)).start(queueLoop, shardId, queueConsumer, queueRunner);

        verify(queueLoop).doRun(any());
        verify(listener).started(shardId, location);
        verify(queueRunner).runQueue(queueConsumer);
        verify(queueLoop).doWait(fatalCrashTimeout, QueueLoop.WaitInterrupt.DENY);
        verify(listener).crashed(shardId, location, exception);
        verify(listener).finished(shardId, location);
    }

}