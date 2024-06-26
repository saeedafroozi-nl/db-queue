package com.bestseller.dbqueue.core.internal.runner;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import com.bestseller.dbqueue.core.api.QueueConsumer;
import com.bestseller.dbqueue.core.api.Task;
import com.bestseller.dbqueue.core.api.TaskExecutionResult;
import com.bestseller.dbqueue.core.config.QueueShard;
import com.bestseller.dbqueue.core.config.QueueShardId;
import com.bestseller.dbqueue.core.config.TaskLifecycleListener;
import com.bestseller.dbqueue.core.settings.ProcessingMode;
import com.bestseller.dbqueue.core.settings.QueueConfig;
import com.bestseller.dbqueue.core.settings.QueueId;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import com.bestseller.dbqueue.core.settings.QueueSettings;
import com.bestseller.dbqueue.core.stub.StringQueueConsumer;
import com.bestseller.dbqueue.core.stub.StubDatabaseAccessLayer;
import com.bestseller.dbqueue.core.stub.TestFixtures;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class QueueRunnerSpringQueuePickTaskDaoQueueDaoFactoryTest {

    private static class ConsumerWithExternalExecutor extends StringQueueConsumer {

        @Nullable
        private final Executor executor;

        ConsumerWithExternalExecutor(@Nonnull QueueConfig queueConfig,
                                     @Nullable Executor executor) {
            super(queueConfig);
            this.executor = executor;
        }

        @Nonnull
        @Override
        public TaskExecutionResult execute(@Nonnull Task<String> task) {
            return TaskExecutionResult.finish();
        }

        @Override
        public Optional<Executor> getExecutor() {
            return Optional.ofNullable(executor);
        }
    }

    @Test
    public void should_return_external_executor_runner() throws Exception {
        QueueSettings settings = TestFixtures.createQueueSettings().withProcessingSettings(
                TestFixtures.createProcessingSettings().withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR).build()).build();
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        QueueConsumer queueConsumer = new ConsumerWithExternalExecutor(new QueueConfig(location, settings), mock(Executor.class));
        QueueRunner queueRunner = QueueRunner.Factory.create(queueConsumer,
                new QueueShard<>(new QueueShardId("s1"), new StubDatabaseAccessLayer()),
                mock(TaskLifecycleListener.class));

        assertThat(queueRunner, CoreMatchers.instanceOf(QueueRunnerInExternalExecutor.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_throw_exception_when_no_external_executor_runner() throws Exception {
        QueueSettings settings = TestFixtures.createQueueSettings().withProcessingSettings(
                TestFixtures.createProcessingSettings().withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR).build()).build();
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        QueueConsumer queueConsumer = new StringQueueConsumer(new QueueConfig(location, settings)) {
            @Nonnull
            @Override
            public TaskExecutionResult execute(@Nonnull Task<String> task) {
                return TaskExecutionResult.finish();
            }
        };
        QueueRunner queueRunner = QueueRunner.Factory.create(queueConsumer,
                new QueueShard<>(new QueueShardId("s1"), new StubDatabaseAccessLayer()),
                mock(TaskLifecycleListener.class));

        assertThat(queueRunner, CoreMatchers.instanceOf(QueueRunnerInExternalExecutor.class));
    }

    @Test
    public void should_return_separate_transactions_runner() throws Exception {
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        QueueSettings settings = TestFixtures.createQueueSettings().withProcessingSettings(
                TestFixtures.createProcessingSettings().withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS).build()).build();
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location, settings));

        QueueRunner queueRunner = QueueRunner.Factory.create(queueConsumer,
                new QueueShard<>(new QueueShardId("s1"), new StubDatabaseAccessLayer()),
                mock(TaskLifecycleListener.class));

        assertThat(queueRunner, CoreMatchers.instanceOf(QueueRunnerInSeparateTransactions.class));
    }

    @Test
    public void should_return_wrap_in_transaction_runner() throws Exception {
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        QueueSettings settings = TestFixtures.createQueueSettings().withProcessingSettings(
                TestFixtures.createProcessingSettings().withProcessingMode(ProcessingMode.WRAP_IN_TRANSACTION).build()).build();
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location, settings));

        QueueRunner queueRunner = QueueRunner.Factory.create(queueConsumer,
                new QueueShard<>(new QueueShardId("s1"), new StubDatabaseAccessLayer()),
                mock(TaskLifecycleListener.class));

        assertThat(queueRunner, CoreMatchers.instanceOf(QueueRunnerInTransaction.class));
    }
}