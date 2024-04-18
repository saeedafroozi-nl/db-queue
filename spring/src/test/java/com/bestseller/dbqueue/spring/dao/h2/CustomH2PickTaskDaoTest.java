package com.bestseller.dbqueue.spring.dao.h2;

import com.bestseller.dbqueue.spring.dao.H2QueueDao;
import com.bestseller.dbqueue.spring.dao.H2QueuePickTaskDao;
import com.bestseller.dbqueue.spring.dao.QueuePickTaskDaoTest;
import com.bestseller.dbqueue.spring.dao.utils.H2DatabaseInitializer;
import org.junit.BeforeClass;


public class CustomH2PickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        H2DatabaseInitializer.initialize();
    }

    public CustomH2PickTaskDaoTest() {
        super(
                new H2QueueDao(
                        H2DatabaseInitializer.getJdbcTemplate(),
                        H2DatabaseInitializer.CUSTOM_SCHEMA),
                (queueLocation, failureSettings) ->
                        new H2QueuePickTaskDao(
                                H2DatabaseInitializer.getJdbcTemplate(),
                                H2DatabaseInitializer.CUSTOM_SCHEMA,
                                queueLocation,
                                failureSettings, getPollSettings()),
                H2DatabaseInitializer.CUSTOM_TABLE_NAME,
                H2DatabaseInitializer.CUSTOM_SCHEMA,
                H2DatabaseInitializer.getJdbcTemplate(),
                H2DatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "now()";
    }

}
