package com.bestseller.dbqueue.spring.dao.mysql;

import com.bestseller.dbqueue.spring.dao.MysqlQueueDao;
import com.bestseller.dbqueue.spring.dao.QueueDaoTest;
import com.bestseller.dbqueue.spring.dao.utils.MysqlDatabaseInitializer;
import org.junit.BeforeClass;

public class CustomMysqlQueueDaoTest extends QueueDaoTest {
    @BeforeClass
    public static void beforeClass() {
        MysqlDatabaseInitializer.initialize();
    }

    public CustomMysqlQueueDaoTest() {
        super(new MysqlQueueDao(MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.CUSTOM_SCHEMA),
                MysqlDatabaseInitializer.CUSTOM_TABLE_NAME, MysqlDatabaseInitializer.CUSTOM_SCHEMA,
                MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.getTransactionTemplate());
    }
}
