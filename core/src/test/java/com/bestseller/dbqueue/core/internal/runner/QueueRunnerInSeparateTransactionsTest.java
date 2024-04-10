package com.bestseller.dbqueue.core.internal.runner;

import org.junit.Test;
import com.bestseller.dbqueue.core.api.QueueConsumer;
import com.bestseller.dbqueue.core.api.TaskRecord;
import com.bestseller.dbqueue.core.internal.processing.QueueProcessingStatus;
import com.bestseller.dbqueue.core.internal.processing.TaskPicker;
import com.bestseller.dbqueue.core.internal.processing.TaskProcessor;
import com.bestseller.dbqueue.core.settings.QueueConfig;
import com.bestseller.dbqueue.core.settings.QueueId;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import com.bestseller.dbqueue.core.stub.TestFixtures;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class QueueRunnerInSeparateTransactionsTest {

    private static final QueueLocation testLocation1 =
            QueueLocation.builder().withTableName("queue_test")
                    .withQueueId(new QueueId("test_queue1")).build();

    @Test
    public void should_wait_notasktimeout_when_no_task_found() {
        Duration betweenTaskTimeout = Duration.ofHours(1L);
        Duration noTaskTimeout = Duration.ofMillis(5L);

        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        TaskPicker taskPicker = mock(TaskPicker.class);
        when(taskPicker.pickTasks()).thenReturn(List.of());
        TaskProcessor taskProcessor = mock(TaskProcessor.class);

        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()).build()));
        QueueProcessingStatus status = new QueueRunnerInSeparateTransactions(taskPicker, taskProcessor).runQueue(queueConsumer);

        assertThat(status, equalTo(QueueProcessingStatus.SKIPPED));

        verify(taskPicker).pickTasks();
        verifyNoInteractions(taskProcessor);
    }

    @Test
    public void should_wait_betweentasktimeout_when_task_found() {
        Duration betweenTaskTimeout = Duration.ofHours(1L);
        Duration noTaskTimeout = Duration.ofMillis(5L);

        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        TaskPicker taskPicker = mock(TaskPicker.class);
        TaskRecord taskRecord = TaskRecord.builder().build();
        when(taskPicker.pickTasks()).thenReturn(List.of(taskRecord));
        TaskProcessor taskProcessor = mock(TaskProcessor.class);


        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()).build()));
        QueueProcessingStatus status = new QueueRunnerInSeparateTransactions(taskPicker, taskProcessor).runQueue(queueConsumer);

        assertThat(status, equalTo(QueueProcessingStatus.PROCESSED));

        verify(taskPicker).pickTasks();
        verify(taskProcessor).processTask(queueConsumer, taskRecord);
    }

}