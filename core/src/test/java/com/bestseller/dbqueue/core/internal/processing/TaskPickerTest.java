package com.bestseller.dbqueue.core.internal.processing;

import org.junit.Test;
import com.bestseller.dbqueue.core.api.QueueConsumer;
import com.bestseller.dbqueue.core.api.TaskRecord;
import com.bestseller.dbqueue.core.config.QueueShard;
import com.bestseller.dbqueue.core.config.QueueShardId;
import com.bestseller.dbqueue.core.config.TaskLifecycleListener;
import com.bestseller.dbqueue.core.dao.QueuePickTaskDao;
import com.bestseller.dbqueue.core.settings.QueueConfig;
import com.bestseller.dbqueue.core.settings.QueueId;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import com.bestseller.dbqueue.core.stub.FakeMillisTimeProvider;
import com.bestseller.dbqueue.core.stub.StubDatabaseAccessLayer;
import com.bestseller.dbqueue.core.stub.TestFixtures;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class TaskPickerTest {

    @Test
    public void should_successfully_pick_task() throws Exception {
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        QueueShardId shardId = new QueueShardId("s1");
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getShardId()).thenReturn(shardId);
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location,
                TestFixtures.createQueueSettings().build()));
        QueuePickTaskDao pickTaskDao = mock(QueuePickTaskDao.class);
        when(queueShard.getDatabaseAccessLayer()).thenReturn(new StubDatabaseAccessLayer());
        TaskRecord taskRecord = TaskRecord.builder().build();
        when(pickTaskDao.pickTasks()).thenReturn(List.of(taskRecord));
        when(queueShard.getShardId()).thenReturn(shardId);
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        FakeMillisTimeProvider millisTimeProvider = spy(new FakeMillisTimeProvider(Arrays.asList(3L, 5L)));

        List<TaskRecord> pickedTasks = new TaskPicker(queueShard, location, listener, millisTimeProvider, pickTaskDao).pickTasks();

        assertThat(pickedTasks, equalTo(List.of(taskRecord)));

        verify(millisTimeProvider, times(2)).getMillis();
        verify(queueShard).getDatabaseAccessLayer();
        verify(pickTaskDao).pickTasks();
        verify(listener).picked(shardId, location, taskRecord, 2L);
    }

    @Test
    public void should_not_notify_when_task_not_picked() throws Exception {
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        QueueShard queueShard = mock(QueueShard.class);
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location,
                TestFixtures.createQueueSettings().build()));
        QueuePickTaskDao pickTaskDao = mock(QueuePickTaskDao.class);
        when(queueShard.getDatabaseAccessLayer()).thenReturn(new StubDatabaseAccessLayer());
        when(pickTaskDao.pickTasks()).thenReturn(List.of());
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        FakeMillisTimeProvider millisTimeProvider = spy(new FakeMillisTimeProvider(Arrays.asList(3L, 5L)));

        List<TaskRecord> pickedTasks = new TaskPicker(queueShard, location, listener, millisTimeProvider, pickTaskDao).pickTasks();

        assertThat(pickedTasks, equalTo(List.of()));

        verify(millisTimeProvider, times(2)).getMillis();
        verify(queueShard).getDatabaseAccessLayer();
        verify(pickTaskDao).pickTasks();
        verifyNoInteractions(listener);
    }

    @Test(expected = IllegalStateException.class)
    public void should_not_catch_exception() throws Exception {
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        QueueShard queueShard = mock(QueueShard.class);
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location,
                TestFixtures.createQueueSettings().build()));
        QueuePickTaskDao pickTaskDao = mock(QueuePickTaskDao.class);
        when(queueShard.getDatabaseAccessLayer()).thenReturn(new StubDatabaseAccessLayer());
        when(pickTaskDao.pickTasks()).thenThrow(new IllegalStateException("fail"));
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        FakeMillisTimeProvider millisTimeProvider = spy(new FakeMillisTimeProvider(Arrays.asList(3L, 5L)));

        List<TaskRecord> pickedTasks = new TaskPicker(queueShard, location, listener, millisTimeProvider, pickTaskDao).pickTasks();

        assertThat(pickedTasks, equalTo(List.of()));

        verify(millisTimeProvider).getMillis();
        verify(queueShard).getDatabaseAccessLayer();
        verify(pickTaskDao).pickTasks();
        verifyNoInteractions(listener);
    }
}