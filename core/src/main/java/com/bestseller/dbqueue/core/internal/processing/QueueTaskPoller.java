package com.bestseller.dbqueue.core.internal.processing;

import com.bestseller.dbqueue.core.api.QueueConsumer;
import com.bestseller.dbqueue.core.config.QueueShardId;
import com.bestseller.dbqueue.core.config.ThreadLifecycleListener;
import com.bestseller.dbqueue.core.internal.runner.QueueRunner;
import com.bestseller.dbqueue.core.settings.PollSettings;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Цикл обработки задачи в очереди.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public class QueueTaskPoller {

    @Nonnull
    private final ThreadLifecycleListener threadLifecycleListener;
    @Nonnull
    private final MillisTimeProvider millisTimeProvider;

    /**
     * Конструктор
     *
     * @param threadLifecycleListener слушатель событий исполнения очереди
     * @param millisTimeProvider      поставщик текущего времени
     */
    public QueueTaskPoller(@Nonnull ThreadLifecycleListener threadLifecycleListener,
                           @Nonnull MillisTimeProvider millisTimeProvider) {
        this.threadLifecycleListener = requireNonNull(threadLifecycleListener);
        this.millisTimeProvider = requireNonNull(millisTimeProvider);
    }

    /**
     * Запустить цикл обработки задач в очереди
     *
     * @param queueLoop     стратегия выполнения цикла
     * @param shardId       идентификатор шарда, на котором происходит обработка
     * @param queueConsumer выполняемая очередь
     * @param queueRunner   исполнитель очереди
     */
    public void start(@Nonnull QueueLoop queueLoop,
                      @Nonnull QueueShardId shardId,
                      @Nonnull QueueConsumer queueConsumer,
                      @Nonnull QueueRunner queueRunner) {
        requireNonNull(shardId);
        requireNonNull(queueConsumer);
        requireNonNull(queueRunner);
        requireNonNull(queueLoop);
        queueLoop.doRun(() -> {
            PollSettings pollSettings = queueConsumer.getQueueConfig().getSettings().getPollSettings();
            try {
                long startTime = millisTimeProvider.getMillis();
                threadLifecycleListener.started(shardId, queueConsumer.getQueueConfig().getLocation());
                QueueProcessingStatus queueProcessingStatus = queueRunner.runQueue(queueConsumer);
                threadLifecycleListener.executed(shardId, queueConsumer.getQueueConfig().getLocation(),
                        queueProcessingStatus != QueueProcessingStatus.SKIPPED,
                        millisTimeProvider.getMillis() - startTime);

                switch (queueProcessingStatus) {
                    case SKIPPED -> {
                        threadLifecycleListener.noTask(shardId, queueConsumer.getQueueConfig().getLocation());
                        queueLoop.doWait(pollSettings.getNoTaskTimeout(), QueueLoop.WaitInterrupt.ALLOW);
                    }
                    case PROCESSED -> {
                        threadLifecycleListener.processed(shardId, queueConsumer.getQueueConfig().getLocation());
                        queueLoop.doWait(pollSettings.getBetweenTaskTimeout(), QueueLoop.WaitInterrupt.DENY);
                    }
                    default -> throw new IllegalStateException("unknown task loop result" + queueProcessingStatus);
                }
            } catch (Throwable e) {
                threadLifecycleListener.crashed(shardId, queueConsumer.getQueueConfig().getLocation(), e);
                queueLoop.doWait(pollSettings.getFatalCrashTimeout(), QueueLoop.WaitInterrupt.DENY);
            } finally {
                threadLifecycleListener.finished(shardId, queueConsumer.getQueueConfig().getLocation());
            }
        });
    }

}
