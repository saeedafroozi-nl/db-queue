package com.bestseller.dbqueue.spring.dao;

import com.bestseller.dbqueue.spring.dao.utils.MysqlDatabaseInitializer;
import org.junit.BeforeClass;

import java.sql.Timestamp;

public class DefaultMysqlQueuePickTaskDaoTest extends QueuePickTaskDaoTest {
    @BeforeClass
    public static void beforeClass() {
        MysqlDatabaseInitializer.initialize();
    }

    public DefaultMysqlQueuePickTaskDaoTest() {
        super(new MssqlQueueDao(MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.DEFAULT_SCHEMA),
                (queueLocation, failureSettings) -> new MssqlQueuePickTaskDao(MysqlDatabaseInitializer.getJdbcTemplate(),
                        MysqlDatabaseInitializer.DEFAULT_SCHEMA, queueLocation, failureSettings, getPollSettings()),
                MysqlDatabaseInitializer.DEFAULT_TABLE_NAME, MysqlDatabaseInitializer.DEFAULT_SCHEMA,
                MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        long currentTimeMillis = System.currentTimeMillis();
        Timestamp currentTimestamp = new Timestamp(currentTimeMillis);
        return currentTimestamp.toString();
    }
}
