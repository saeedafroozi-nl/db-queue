package com.bestseller.dbqueue.spring.dao;

import com.bestseller.dbqueue.core.api.EnqueueParams;
import com.bestseller.dbqueue.core.api.TaskRecord;
import com.bestseller.dbqueue.core.config.QueueTableSchema;
import com.bestseller.dbqueue.core.dao.QueueDao;
import com.bestseller.dbqueue.core.dao.QueuePickTaskDao;
import com.bestseller.dbqueue.core.settings.FailRetryType;
import com.bestseller.dbqueue.core.settings.FailureSettings;
import com.bestseller.dbqueue.core.settings.PollSettings;
import com.bestseller.dbqueue.core.settings.QueueId;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.shaded.org.hamcrest.Matchers;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

/**
 * @author Oleg Kandaurov
 * @since 15.07.2017
 */
@Ignore
public abstract class QueuePickTaskDaoTest {

    protected final NamedParameterJdbcTemplate jdbcTemplate;
    protected final TransactionTemplate transactionTemplate;

    protected final String tableName;
    protected final QueueTableSchema tableSchema;

    protected final QueueDao queueDao;
    protected final BiFunction<QueueLocation, FailureSettings, QueuePickTaskDao> pickTaskDaoFactory;

    /**
     * Some glitches with Windows
     */
    private static final Duration WINDOWS_OS_DELAY = Duration.ofSeconds(2);

    public QueuePickTaskDaoTest(QueueDao queueDao,
                                BiFunction<QueueLocation, FailureSettings, QueuePickTaskDao> pickTaskDaoFactory,
                                String tableName, QueueTableSchema tableSchema,
                                JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.transactionTemplate = transactionTemplate;
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(jdbcTemplate));
        this.queueDao = queueDao;
        this.pickTaskDaoFactory = pickTaskDaoFactory;
    }

    @Test
    public void should_not_pick_task_too_early() throws Exception {
        QueueLocation location = generateUniqueLocation();
        executeInTransaction(() ->
                queueDao.enqueue(location, new EnqueueParams<String>().withExecutionDelay(Duration.ofHours(1))));
        QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(location, FailureSettings.builder()
                .withRetryType(FailRetryType.ARITHMETIC_BACKOFF)
                .withRetryInterval(Duration.ofMinutes(1)).build());
        List<TaskRecord> taskRecord = pickTaskDao.pickTasks();
        Assert.assertThat(taskRecord, is(not(Matchers.empty())));
    }

    @Test
    public void pick_task_should_return_all_fields() throws Exception {
        QueueLocation location = generateUniqueLocation();
        String payload = "{}";
        ZonedDateTime beforeEnqueue = ZonedDateTime.now().minusMinutes(1L);
        long enqueueId = executeInTransaction(() -> queueDao.enqueue(location,
                EnqueueParams.create(payload)));

        List<TaskRecord> taskRecords = List.of();
        QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(location, FailureSettings.builder()
                .withRetryType(FailRetryType.ARITHMETIC_BACKOFF)
                .withRetryInterval(Duration.ofMinutes(1)).build());
        while (taskRecords.isEmpty()) {
            taskRecords = executeInTransaction(pickTaskDao::pickTasks);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        Assert.assertThat(taskRecords.size(), equalTo(1));
        TaskRecord taskRecord = taskRecords.get(0);

        ZonedDateTime afterEnqueue = ZonedDateTime.now().plusMinutes(1L);
        Objects.requireNonNull(taskRecord);
        Assert.assertThat(taskRecord.getAttemptsCount(), equalTo(1L));
        Assert.assertThat(taskRecord.getId(), equalTo(enqueueId));
        Assert.assertThat(taskRecord.getPayload(), equalTo(payload));
        Assert.assertThat(taskRecord.getNextProcessAt(), is(not(nullValue())));
        Assert.assertThat(taskRecord.getCreatedAt().isAfter(beforeEnqueue), equalTo(true));
        Assert.assertThat(taskRecord.getCreatedAt().isBefore(afterEnqueue), equalTo(true));
    }

    @Test
    public void pick_task_should_delay_with_linear_strategy() {
        QueueLocation location = generateUniqueLocation();
        Duration expectedDelay = Duration.ofMinutes(3L);
        ZonedDateTime beforePickingTask;
        ZonedDateTime afterPickingTask;
        TaskRecord taskRecord;
        QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(location, FailureSettings.builder()
                .withRetryType(FailRetryType.LINEAR_BACKOFF)
                .withRetryInterval(Duration.ofMinutes(3)).build());

        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        for (int attempt = 1; attempt < 10; attempt++) {
            beforePickingTask = ZonedDateTime.now();
            taskRecord = resetProcessTimeAndPick(pickTaskDao, enqueueId);
            afterPickingTask = ZonedDateTime.now();
            Assert.assertThat(taskRecord.getAttemptsCount(), equalTo((long) attempt));
            Assert.assertThat(taskRecord.getNextProcessAt().isAfter(beforePickingTask.plus(expectedDelay.minus(WINDOWS_OS_DELAY))), equalTo(true));
            Assert.assertThat(taskRecord.getNextProcessAt().isBefore(afterPickingTask.plus(expectedDelay).plus(WINDOWS_OS_DELAY)), equalTo(true));
        }
    }

    @Test
    public void pick_task_should_delay_with_linear_delay_after_setting_changed() {
        QueueLocation location = generateUniqueLocation();
        Duration expectedDelay = Duration.ofMinutes(3L);
        ZonedDateTime beforePickingTask;
        ZonedDateTime afterPickingTask;
        TaskRecord taskRecord;
        FailureSettings failureSettings = FailureSettings.builder()
                .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
                .withRetryInterval(Duration.ofMinutes(99)).build();
        QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(location, failureSettings);

        failureSettings.setValue(FailureSettings.builder()
                .withRetryType(FailRetryType.LINEAR_BACKOFF)
                .withRetryInterval(Duration.ofMinutes(3)).build());

        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        for (int attempt = 1; attempt < 10; attempt++) {
            beforePickingTask = ZonedDateTime.now();
            taskRecord = resetProcessTimeAndPick(pickTaskDao, enqueueId);
            afterPickingTask = ZonedDateTime.now();
            Assert.assertThat(taskRecord.getAttemptsCount(), equalTo((long) attempt));
            Assert.assertThat(taskRecord.getNextProcessAt().isAfter(beforePickingTask.plus(expectedDelay.minus(WINDOWS_OS_DELAY))), equalTo(true));
            Assert.assertThat(taskRecord.getNextProcessAt().isBefore(afterPickingTask.plus(expectedDelay).plus(WINDOWS_OS_DELAY)), equalTo(true));
        }
    }

    @Test
    public void pick_task_should_delay_with_arithmetic_strategy() {
        QueueLocation location = generateUniqueLocation();
        Duration expectedDelay;
        ZonedDateTime beforePickingTask;
        ZonedDateTime afterPickingTask;
        TaskRecord taskRecord;

        QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(location, FailureSettings.builder()
                .withRetryType(FailRetryType.ARITHMETIC_BACKOFF)
                .withRetryInterval(Duration.ofMinutes(1)).build());

        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        for (int attempt = 1; attempt < 10; attempt++) {
            beforePickingTask = ZonedDateTime.now();
            taskRecord = resetProcessTimeAndPick(pickTaskDao, enqueueId);
            afterPickingTask = ZonedDateTime.now();
            expectedDelay = Duration.ofMinutes(1 + (attempt - 1) * 2);
            Assert.assertThat(taskRecord.getAttemptsCount(), equalTo((long) attempt));
            Assert.assertThat(taskRecord.getNextProcessAt().isAfter(beforePickingTask.plus(expectedDelay.minus(WINDOWS_OS_DELAY))), equalTo(true));
            Assert.assertThat(taskRecord.getNextProcessAt().isBefore(afterPickingTask.plus(expectedDelay.plus(WINDOWS_OS_DELAY))), equalTo(true));
        }
    }

    @Test
    public void pick_task_should_delay_with_geometric_strategy() {
        QueueLocation location = generateUniqueLocation();
        Duration expectedDelay;
        ZonedDateTime beforePickingTask;
        ZonedDateTime afterPickingTask;
        TaskRecord taskRecord;

        QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(location, FailureSettings.builder()
                .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
                .withRetryInterval(Duration.ofMinutes(1)).build());

        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        for (int attempt = 1; attempt < 10; attempt++) {
            beforePickingTask = ZonedDateTime.now();
            taskRecord = resetProcessTimeAndPick(pickTaskDao, enqueueId);
            afterPickingTask = ZonedDateTime.now();
            expectedDelay = Duration.ofMinutes(BigInteger.valueOf(2L).pow(attempt - 1).longValue());
            Assert.assertThat(taskRecord.getAttemptsCount(), equalTo((long) attempt));
            Assert.assertThat(taskRecord.getNextProcessAt().isAfter(beforePickingTask.plus(expectedDelay.minus(WINDOWS_OS_DELAY))), equalTo(true));
            Assert.assertThat(taskRecord.getNextProcessAt().isBefore(afterPickingTask.plus(expectedDelay.plus(WINDOWS_OS_DELAY))), equalTo(true));
        }
    }

    private TaskRecord resetProcessTimeAndPick(QueuePickTaskDao pickTaskDao, Long enqueueId) {
        String sqlQuery = "update " + tableName +
                " set " + tableSchema.getNextProcessAtField() + " = " + currentTimeSql() + " where " + tableSchema.getIdField() + " = :enqueueId";
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("enqueueId", enqueueId);

        executeInTransaction(() -> {
            jdbcTemplate.update(sqlQuery, params);
        });

        List<TaskRecord> taskRecords = List.of();
        while (taskRecords.isEmpty()) {
            taskRecords = executeInTransaction(
                    pickTaskDao::pickTasks);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        Assert.assertThat(taskRecords.size(), equalTo(1));
        return taskRecords.get(0);
    }

    protected abstract String currentTimeSql();

    protected QueueLocation generateUniqueLocation() {
        return QueueLocation.builder().withTableName(tableName)
                .withQueueId(new QueueId("test-queue-" + UUID.randomUUID())).build();
    }

    protected void executeInTransaction(Runnable runnable) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                runnable.run();
            }
        });
    }

    protected <T> T executeInTransaction(Supplier<T> supplier) {
        return transactionTemplate.execute(status -> supplier.get());
    }

    protected static PollSettings getPollSettings() {
        return PollSettings.builder().withBetweenTaskTimeout(Duration.ofSeconds(4))
                .withNoTaskTimeout(Duration.ofSeconds(5)).withFatalCrashTimeout(Duration.ofSeconds(6))
                .withBatchSize(1).withQueryVersion(0).build();
    }

}
