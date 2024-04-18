package com.bestseller.dbqueue.spring.dao.postgres;

import com.bestseller.dbqueue.spring.dao.PostgresQueueDao;
import com.bestseller.dbqueue.spring.dao.PostgresQueuePickTaskDao;
import com.bestseller.dbqueue.spring.dao.QueuePickTaskDaoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import com.bestseller.dbqueue.core.api.EnqueueParams;
import com.bestseller.dbqueue.core.api.TaskRecord;
import com.bestseller.dbqueue.core.dao.QueuePickTaskDao;
import com.bestseller.dbqueue.core.settings.FailRetryType;
import com.bestseller.dbqueue.core.settings.FailureSettings;
import com.bestseller.dbqueue.core.settings.PollSettings;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import com.bestseller.dbqueue.spring.dao.utils.PostgresDatabaseInitializer;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class DefaultPostgresQueuePickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        PostgresDatabaseInitializer.initialize();
    }

    public DefaultPostgresQueuePickTaskDaoTest() {
        super(new PostgresQueueDao(PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.DEFAULT_SCHEMA),
                (queueLocation, failureSettings) -> new PostgresQueuePickTaskDao(PostgresDatabaseInitializer.getJdbcTemplate(),
                        PostgresDatabaseInitializer.DEFAULT_SCHEMA, queueLocation, failureSettings, getPollSettings()),
                PostgresDatabaseInitializer.DEFAULT_TABLE_NAME, PostgresDatabaseInitializer.DEFAULT_SCHEMA,
                PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "now()";
    }

    @Test
    public void should_pick_tasks_batch() {
        QueueLocation location = generateUniqueLocation();
        PollSettings pollSettings = PollSettings.builder().withBetweenTaskTimeout(Duration.ofSeconds(4))
                .withNoTaskTimeout(Duration.ofSeconds(5)).withFatalCrashTimeout(Duration.ofSeconds(6))
                .withBatchSize(2)
                .withQueryVersion(0).build();
        FailureSettings failureSettings = FailureSettings.builder()
                .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
                .withRetryInterval(Duration.ofMinutes(1)).build();
        QueuePickTaskDao pickTaskDao = new PostgresQueuePickTaskDao(PostgresDatabaseInitializer.getJdbcTemplate(),
                PostgresDatabaseInitializer.DEFAULT_SCHEMA, location, failureSettings, pollSettings);

        executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));
        executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        List<TaskRecord> taskRecords = executeInTransaction(pickTaskDao::pickTasks);
        Assert.assertThat(taskRecords.size(), equalTo(2));
    }
}
