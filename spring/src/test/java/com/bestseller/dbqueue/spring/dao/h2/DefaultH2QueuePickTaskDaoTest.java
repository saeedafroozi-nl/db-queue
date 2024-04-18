package com.bestseller.dbqueue.spring.dao.h2;

import com.bestseller.dbqueue.spring.dao.H2QueueDao;
import com.bestseller.dbqueue.spring.dao.H2QueuePickTaskDao;
import com.bestseller.dbqueue.spring.dao.QueuePickTaskDaoTest;
import org.junit.BeforeClass;
import com.bestseller.dbqueue.spring.dao.utils.H2DatabaseInitializer;

public class DefaultH2QueuePickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        H2DatabaseInitializer.initialize();
    }

    public DefaultH2QueuePickTaskDaoTest() {
        super(
                new H2QueueDao(H2DatabaseInitializer.getJdbcTemplate(), H2DatabaseInitializer.DEFAULT_SCHEMA),
                (queueLocation, failureSettings) ->
                        new H2QueuePickTaskDao(
                                H2DatabaseInitializer.getJdbcTemplate(),
                                H2DatabaseInitializer.DEFAULT_SCHEMA,
                                queueLocation, failureSettings, getPollSettings()),
                H2DatabaseInitializer.DEFAULT_TABLE_NAME, H2DatabaseInitializer.DEFAULT_SCHEMA,
                H2DatabaseInitializer.getJdbcTemplate(), H2DatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "now()";
    }
}
