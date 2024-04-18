package com.bestseller.dbqueue.spring.dao.postgres;

import com.bestseller.dbqueue.spring.dao.PostgresQueueDao;
import com.bestseller.dbqueue.spring.dao.PostgresQueuePickTaskDao;
import com.bestseller.dbqueue.spring.dao.QueuePickTaskDaoTest;
import org.junit.BeforeClass;
import com.bestseller.dbqueue.spring.dao.utils.PostgresDatabaseInitializer;

public class CustomPostgresQueuePickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        PostgresDatabaseInitializer.initialize();
    }

    public CustomPostgresQueuePickTaskDaoTest() {
        super(new PostgresQueueDao(PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.CUSTOM_SCHEMA),
                (queueLocation, failureSettings) -> new PostgresQueuePickTaskDao(PostgresDatabaseInitializer.getJdbcTemplate(),
                        PostgresDatabaseInitializer.CUSTOM_SCHEMA, queueLocation, failureSettings, getPollSettings()),
                PostgresDatabaseInitializer.CUSTOM_TABLE_NAME, PostgresDatabaseInitializer.CUSTOM_SCHEMA,
                PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "now()";
    }
}
