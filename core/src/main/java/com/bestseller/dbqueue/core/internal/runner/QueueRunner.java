package com.bestseller.dbqueue.core.internal.runner;

import com.bestseller.dbqueue.core.internal.processing.MillisTimeProvider;
import com.bestseller.dbqueue.core.internal.processing.QueueProcessingStatus;
import com.bestseller.dbqueue.core.internal.processing.TaskPicker;
import com.bestseller.dbqueue.core.internal.processing.TaskProcessor;
import com.bestseller.dbqueue.core.internal.processing.TaskResultHandler;
import com.bestseller.dbqueue.core.api.QueueConsumer;
import com.bestseller.dbqueue.core.config.QueueShard;
import com.bestseller.dbqueue.core.config.TaskLifecycleListener;
import com.bestseller.dbqueue.core.dao.QueuePickTaskDao;
import com.bestseller.dbqueue.core.settings.ProcessingMode;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import com.bestseller.dbqueue.core.settings.QueueSettings;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

/**
 * Интерфейс обработчика пула задач очереди
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
@FunctionalInterface
public interface QueueRunner {

    /**
     * Единократно обработать заданную очередь
     *
     * @param queueConsumer очередь для обработки
     * @return тип результата выполнения задачи
     */
    @Nonnull
    QueueProcessingStatus runQueue(@Nonnull QueueConsumer queueConsumer);

    /**
     * Фабрика исполнителей задач в очереди
     */
    final class Factory {

        private Factory() {
        }

        /**
         * Создать исполнителя задач очереди
         *
         * @param queueConsumer         очередь обработки задач
         * @param queueShard            шард, на котором будут запущен consumer
         * @param taskLifecycleListener слушатель процесса обработки задач
         * @return инстанс исполнителя задач
         */
        public static QueueRunner create(@Nonnull QueueConsumer queueConsumer,
                                         @Nonnull QueueShard<?> queueShard,
                                         @Nonnull TaskLifecycleListener taskLifecycleListener) {
            requireNonNull(queueConsumer);
            requireNonNull(queueShard);
            requireNonNull(taskLifecycleListener);

            QueueSettings queueSettings = queueConsumer.getQueueConfig().getSettings();
            QueueLocation queueLocation = queueConsumer.getQueueConfig().getLocation();

            QueuePickTaskDao queuePickTaskDao = queueShard.getDatabaseAccessLayer().createQueuePickTaskDao(
                    queueLocation,
                    queueSettings.getFailureSettings(),
                    queueSettings.getPollSettings()
            );

            TaskPicker taskPicker = new TaskPicker(queueShard, queueLocation, taskLifecycleListener,
                    new MillisTimeProvider.SystemMillisTimeProvider(), queuePickTaskDao);

            TaskResultHandler taskResultHandler = new TaskResultHandler(
                    queueLocation,
                    queueShard, queueSettings.getReenqueueSettings());

            TaskProcessor taskProcessor = new TaskProcessor(queueShard, taskLifecycleListener,
                    new MillisTimeProvider.SystemMillisTimeProvider(), taskResultHandler);

            ProcessingMode processingMode = queueSettings.getProcessingSettings().getProcessingMode();
            return switch (processingMode) {
                case SEPARATE_TRANSACTIONS -> new QueueRunnerInSeparateTransactions(taskPicker, taskProcessor);
                case WRAP_IN_TRANSACTION -> new QueueRunnerInTransaction(taskPicker, taskProcessor, queueShard);
                case USE_EXTERNAL_EXECUTOR -> new QueueRunnerInExternalExecutor(taskPicker, taskProcessor,
                        ((Optional<Executor>) queueConsumer.getExecutor()).orElseThrow(() ->
                                new IllegalArgumentException("Executor is empty. " +
                                        "You must provide QueueConsumer#getExecutor in ProcessingMode#USE_EXTERNAL_EXECUTOR")));
                default -> throw new IllegalStateException("unknown processing mode: " + processingMode);
            };
        }

    }
}
