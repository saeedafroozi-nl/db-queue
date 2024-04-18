package com.bestseller.dbqueue.spring.dao.h2;

import com.bestseller.dbqueue.spring.dao.H2QueueDao;
import com.bestseller.dbqueue.spring.dao.QueueDaoTest;
import org.junit.BeforeClass;
import com.bestseller.dbqueue.spring.dao.utils.H2DatabaseInitializer;

public class DefaultH2QueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        H2DatabaseInitializer.initialize();
    }

    public DefaultH2QueueDaoTest() {
        super(
                new H2QueueDao(H2DatabaseInitializer.getJdbcTemplate(), H2DatabaseInitializer.DEFAULT_SCHEMA),
                H2DatabaseInitializer.DEFAULT_TABLE_NAME,
                H2DatabaseInitializer.DEFAULT_SCHEMA,
                H2DatabaseInitializer.getJdbcTemplate(),
                H2DatabaseInitializer.getTransactionTemplate());
    }
}
