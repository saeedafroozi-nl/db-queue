package com.bestseller.dbqueue.spring.dao;

import com.bestseller.dbqueue.spring.dao.utils.MysqlDatabaseInitializer;
import org.junit.BeforeClass;

public class DefaultMysqlQueueDaoTest extends QueueDaoTest {
    @BeforeClass
    public static void beforeClass() {
        MysqlDatabaseInitializer.initialize();
    }

    public DefaultMysqlQueueDaoTest() {
        super(new MysqlQueueDao(MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.DEFAULT_SCHEMA),
                MysqlDatabaseInitializer.DEFAULT_TABLE_NAME, MysqlDatabaseInitializer.DEFAULT_SCHEMA,
                MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.getTransactionTemplate());
    }
}
