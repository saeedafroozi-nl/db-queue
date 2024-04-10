package com.bestseller.dbqueue.core.api.impl;

import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;

import com.bestseller.dbqueue.core.api.EnqueueParams;
import com.bestseller.dbqueue.core.api.EnqueueResult;
import com.bestseller.dbqueue.core.api.QueueProducer;
import com.bestseller.dbqueue.core.api.TaskPayloadTransformer;
import com.bestseller.dbqueue.core.internal.processing.MillisTimeProvider;
import com.bestseller.dbqueue.core.settings.QueueId;
import lombok.extern.slf4j.Slf4j;


/**
 * Wrapper for queue producer with logging and monitoring support
 *
 * @param <PayloadT> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
@Slf4j
public class MonitoringQueueProducer<PayloadT> implements QueueProducer<PayloadT> {



    @Nonnull
    private final QueueProducer<PayloadT> queueProducer;
    @Nonnull
    private final QueueId queueId;
    @Nonnull
    private final BiConsumer<EnqueueResult, Long> monitoringCallback;
    @Nonnull
    private final MillisTimeProvider millisTimeProvider;

    /**
     * Constructor
     *
     * @param queueProducer      Task producer for the queue
     * @param queueId            Id of the queue
     * @param monitoringCallback Callback invoked after putting a task in the queue.
     *                           It might help to monitor enqueue time.
     * @param millisTimeProvider A millis provider to mock current time
     */
    MonitoringQueueProducer(@Nonnull QueueProducer<PayloadT> queueProducer,
                            @Nonnull QueueId queueId,
                            @Nonnull BiConsumer<EnqueueResult, Long> monitoringCallback,
                            @Nonnull MillisTimeProvider millisTimeProvider) {
        this.queueProducer = Objects.requireNonNull(queueProducer);
        this.queueId = Objects.requireNonNull(queueId);
        this.monitoringCallback = Objects.requireNonNull(monitoringCallback);
        this.millisTimeProvider = Objects.requireNonNull(millisTimeProvider);
    }

    /**
     * Constructor
     *
     * @param queueProducer      Task producer for the queue
     * @param queueId            Id of the queue
     * @param monitoringCallback Callback invoked after putting a task in the queue.
     *                           It might help to monitor enqueue time.
     */
    public MonitoringQueueProducer(@Nonnull QueueProducer<PayloadT> queueProducer,
                                   @Nonnull QueueId queueId,
                                   @Nonnull BiConsumer<EnqueueResult, Long> monitoringCallback) {
        this(queueProducer, queueId, monitoringCallback, new MillisTimeProvider.SystemMillisTimeProvider());
    }

    /**
     * Constructor
     *
     * @param queueProducer Task producer for the queue
     * @param queueId       Id of the queue
     */
    public MonitoringQueueProducer(@Nonnull QueueProducer<PayloadT> queueProducer,
                                   @Nonnull QueueId queueId) {
        this(queueProducer, queueId, (enqueueResult, id) -> {
        });
    }

    @Override
    public EnqueueResult enqueue(@Nonnull EnqueueParams<PayloadT> enqueueParams) {
        log.info("enqueuing task: queue={}, delay={}", queueId, enqueueParams.getExecutionDelay());
        long startTime = millisTimeProvider.getMillis();
        EnqueueResult enqueueResult = queueProducer.enqueue(enqueueParams);
        log.info("task enqueued: id={}, queueShardId={}", enqueueResult.getEnqueueId(), enqueueResult.getShardId());
        long elapsedTime = millisTimeProvider.getMillis() - startTime;
        monitoringCallback.accept(enqueueResult, elapsedTime);
        return enqueueResult;
    }

    @Override
    public void enqueueBatch(@Nonnull List<EnqueueParams<PayloadT>> enqueueParams) {
        log.info("enqueuing tasks batch: queue={}, batchSize={}", queueId, enqueueParams.size());
        long startTime = millisTimeProvider.getMillis();
        queueProducer.enqueueBatch(enqueueParams);
        long elapsedTime = millisTimeProvider.getMillis() - startTime;
        log.info("batch enqueued: time(millis)={}", elapsedTime);
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<PayloadT> getPayloadTransformer() {
        return queueProducer.getPayloadTransformer();
    }
}
