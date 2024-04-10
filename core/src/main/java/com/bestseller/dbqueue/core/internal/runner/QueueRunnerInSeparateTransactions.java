package com.bestseller.dbqueue.core.internal.runner;

import com.bestseller.dbqueue.core.internal.processing.QueueProcessingStatus;
import com.bestseller.dbqueue.core.internal.processing.TaskPicker;
import com.bestseller.dbqueue.core.internal.processing.TaskProcessor;
import com.bestseller.dbqueue.core.api.QueueConsumer;
import com.bestseller.dbqueue.core.api.TaskRecord;
import com.bestseller.dbqueue.core.settings.ProcessingMode;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Исполнитель задач очереди в режиме
 * {@link ProcessingMode#SEPARATE_TRANSACTIONS}
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
class QueueRunnerInSeparateTransactions implements QueueRunner {

    @Nonnull
    private final TaskPicker taskPicker;
    @Nonnull
    private final TaskProcessor taskProcessor;

    /**
     * Конструктор
     *
     * @param taskPicker    выборщик задачи
     * @param taskProcessor обработчик задачи
     */
    QueueRunnerInSeparateTransactions(@Nonnull TaskPicker taskPicker,
                                      @Nonnull TaskProcessor taskProcessor) {
        this.taskPicker = requireNonNull(taskPicker);
        this.taskProcessor = requireNonNull(taskProcessor);

    }

    @Override
    @Nonnull
    public QueueProcessingStatus runQueue(@Nonnull QueueConsumer queueConsumer) {
        List<TaskRecord> taskRecords = taskPicker.pickTasks();
        if (taskRecords.isEmpty()) {
            return QueueProcessingStatus.SKIPPED;
        }
        taskRecords.forEach(taskRecord -> taskProcessor.processTask(queueConsumer, taskRecord));
        return QueueProcessingStatus.PROCESSED;
    }

}
