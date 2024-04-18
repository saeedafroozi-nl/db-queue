package com.bestseller.dbqueue.spring.dao.postgres;

import com.bestseller.dbqueue.spring.dao.PostgresQueueDao;
import com.bestseller.dbqueue.spring.dao.QueueDaoTest;
import org.junit.BeforeClass;
import com.bestseller.dbqueue.core.settings.QueueId;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import com.bestseller.dbqueue.spring.dao.utils.PostgresDatabaseInitializer;

import java.util.UUID;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class DefaultPostgresWithSequenceQueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        PostgresDatabaseInitializer.initialize();
    }

    public DefaultPostgresWithSequenceQueueDaoTest() {
        super(new PostgresQueueDao(PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.DEFAULT_SCHEMA),
                PostgresDatabaseInitializer.DEFAULT_TABLE_NAME_WO_INC, PostgresDatabaseInitializer.DEFAULT_SCHEMA,
                PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.getTransactionTemplate());
    }

    protected QueueLocation generateUniqueLocation() {
        return QueueLocation.builder().withTableName(tableName)
                .withQueueId(new QueueId("test-queue-" + UUID.randomUUID()))
                .withIdSequence("tasks_seq").build();
    }
}
