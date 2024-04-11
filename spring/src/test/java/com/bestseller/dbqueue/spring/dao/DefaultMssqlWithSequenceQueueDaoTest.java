//package com.bestseller.dbqueue.spring.dao;
//
//import org.junit.BeforeClass;
//import com.bestseller.dbqueue.core.settings.QueueId;
//import com.bestseller.dbqueue.core.settings.QueueLocation;
//import com.bestseller.dbqueue.spring.dao.utils.MssqlDatabaseInitializer;
//
//import java.util.UUID;
//
///**
// * @author Oleg Kandaurov
// * @author Behrooz Shabani
// * @since 25.01.2020
// */
//public class DefaultMssqlWithSequenceQueueDaoTest extends QueueDaoTest {
//
//    @BeforeClass
//    public static void beforeClass() {
//        MssqlDatabaseInitializer.initialize();
//    }
//
//    public DefaultMssqlWithSequenceQueueDaoTest() {
//        super(new MssqlQueueDao(MssqlDatabaseInitializer.getJdbcTemplate(), MssqlDatabaseInitializer.DEFAULT_SCHEMA),
//                MssqlDatabaseInitializer.DEFAULT_TABLE_NAME_WO_IDENT, MssqlDatabaseInitializer.DEFAULT_SCHEMA,
//                MssqlDatabaseInitializer.getJdbcTemplate(), MssqlDatabaseInitializer.getTransactionTemplate());
//    }
//
//    @Override
//    protected QueueLocation generateUniqueLocation() {
//        return QueueLocation.builder().withTableName(tableName)
//                .withQueueId(new QueueId("test-queue-" + UUID.randomUUID()))
//                .withIdSequence("tasks_seq").build();
//    }
//}
