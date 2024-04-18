package com.bestseller.dbqueue.spring.dao.postgres;

import com.bestseller.dbqueue.spring.dao.PostgresQueueDao;
import com.bestseller.dbqueue.spring.dao.PostgresQueuePickTaskDao;
import com.bestseller.dbqueue.spring.dao.QueuePickTaskDaoTest;
import org.junit.BeforeClass;
import com.bestseller.dbqueue.core.settings.PollSettings;
import com.bestseller.dbqueue.spring.dao.utils.PostgresDatabaseInitializer;

import java.time.Duration;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class CustomPostgresQueuePickTaskDaoWithoutCTETest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        PostgresDatabaseInitializer.initialize();
    }

    public CustomPostgresQueuePickTaskDaoWithoutCTETest() {
        super(new PostgresQueueDao(PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.CUSTOM_SCHEMA),
                (queueLocation, failureSettings) -> new PostgresQueuePickTaskDao(PostgresDatabaseInitializer.getJdbcTemplate(),
                        PostgresDatabaseInitializer.CUSTOM_SCHEMA, queueLocation, failureSettings, getPollSettingsWithoutCTE()),
                PostgresDatabaseInitializer.CUSTOM_TABLE_NAME, PostgresDatabaseInitializer.CUSTOM_SCHEMA,
                PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "now()";
    }

    protected static PollSettings getPollSettingsWithoutCTE() {
        return PollSettings.builder().withBetweenTaskTimeout(Duration.ofSeconds(4))
                .withNoTaskTimeout(Duration.ofSeconds(5)).withFatalCrashTimeout(Duration.ofSeconds(6))
                .withBatchSize(1).withQueryVersion(1).build();
    }
}
