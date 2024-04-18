package com.bestseller.dbqueue.spring.dao.mysql;

import com.bestseller.dbqueue.spring.dao.MysqlQueueDao;
import com.bestseller.dbqueue.spring.dao.MysqlQueuePickTaskDao;
import com.bestseller.dbqueue.spring.dao.QueuePickTaskDaoTest;
import com.bestseller.dbqueue.spring.dao.utils.MysqlDatabaseInitializer;
import org.junit.BeforeClass;

public class CustomMysqlQueuePickTaskDaoTest extends QueuePickTaskDaoTest {
    @BeforeClass
    public static void beforeClass() {
        MysqlDatabaseInitializer.initialize();
    }

    public CustomMysqlQueuePickTaskDaoTest() {
        super(new MysqlQueueDao(MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.CUSTOM_SCHEMA),
                (queueLocation, failureSettings) -> new MysqlQueuePickTaskDao(MysqlDatabaseInitializer.getJdbcTemplate(),
                        MysqlDatabaseInitializer.CUSTOM_SCHEMA, queueLocation, failureSettings, getPollSettings()),
                MysqlDatabaseInitializer.CUSTOM_TABLE_NAME, MysqlDatabaseInitializer.CUSTOM_SCHEMA,
                MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "CURRENT_TIMESTAMP()";
    }
}
