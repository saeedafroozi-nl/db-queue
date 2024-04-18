package com.bestseller.dbqueue.spring.dao.h2;

import com.bestseller.dbqueue.spring.dao.H2QueueDao;
import com.bestseller.dbqueue.spring.dao.QueueDaoTest;
import com.bestseller.dbqueue.spring.dao.utils.H2DatabaseInitializer;
import org.junit.BeforeClass;


public class CustomH2QueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        H2DatabaseInitializer.initialize();
    }

    public CustomH2QueueDaoTest() {
        super(
                new H2QueueDao(H2DatabaseInitializer.getJdbcTemplate(), H2DatabaseInitializer.CUSTOM_SCHEMA),
                H2DatabaseInitializer.CUSTOM_TABLE_NAME,
                H2DatabaseInitializer.CUSTOM_SCHEMA,
                H2DatabaseInitializer.getJdbcTemplate(),
                H2DatabaseInitializer.getTransactionTemplate());
    }
}
