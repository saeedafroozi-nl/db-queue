package com.bestseller.dbqueue.spring.dao.h2;

import com.bestseller.dbqueue.spring.dao.H2QueueDao;
import com.bestseller.dbqueue.spring.dao.QueueDaoTest;
import org.junit.BeforeClass;
import com.bestseller.dbqueue.core.settings.QueueId;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import com.bestseller.dbqueue.spring.dao.utils.H2DatabaseInitializer;

import java.util.UUID;


public class DefaultH2WithSequenceQueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        H2DatabaseInitializer.initialize();
    }

    public DefaultH2WithSequenceQueueDaoTest() {
        super(
                new H2QueueDao(
                        H2DatabaseInitializer.getJdbcTemplate(),
                        H2DatabaseInitializer.DEFAULT_SCHEMA),
                H2DatabaseInitializer.DEFAULT_TABLE_NAME_WO_INC,
                H2DatabaseInitializer.DEFAULT_SCHEMA,
                H2DatabaseInitializer.getJdbcTemplate(),
                H2DatabaseInitializer.getTransactionTemplate());
    }

    protected QueueLocation generateUniqueLocation() {
        return QueueLocation.builder().withTableName(tableName)
                .withQueueId(new QueueId("test-queue-" + UUID.randomUUID()))
                .withIdSequence("tasks_seq").build();
    }
}