package com.bestseller.dbqueue.spring.dao.mysql;

import com.bestseller.dbqueue.spring.dao.MysqlQueueDao;
import com.bestseller.dbqueue.spring.dao.MysqlQueuePickTaskDao;
import com.bestseller.dbqueue.spring.dao.QueuePickTaskDaoTest;
import com.bestseller.dbqueue.spring.dao.utils.MysqlDatabaseInitializer;
import org.junit.BeforeClass;


public class DefaultMysqlQueuePickTaskDaoTest extends QueuePickTaskDaoTest {
    @BeforeClass
    public static void beforeClass() {
        MysqlDatabaseInitializer.initialize();
    }

    public DefaultMysqlQueuePickTaskDaoTest() {
        super(new MysqlQueueDao(MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.DEFAULT_SCHEMA),
                (queueLocation, failureSettings) -> new MysqlQueuePickTaskDao(MysqlDatabaseInitializer.getJdbcTemplate(),
                        MysqlDatabaseInitializer.DEFAULT_SCHEMA, queueLocation, failureSettings, getPollSettings()),
                MysqlDatabaseInitializer.DEFAULT_TABLE_NAME, MysqlDatabaseInitializer.DEFAULT_SCHEMA,
                MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "CURRENT_TIMESTAMP()";
    }
}
