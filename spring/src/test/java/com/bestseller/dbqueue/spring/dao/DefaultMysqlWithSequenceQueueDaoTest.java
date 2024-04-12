//package com.bestseller.dbqueue.spring.dao;
//
//import com.bestseller.dbqueue.core.settings.QueueId;
//import com.bestseller.dbqueue.core.settings.QueueLocation;
//import com.bestseller.dbqueue.spring.dao.utils.MysqlDatabaseInitializer;
//import org.junit.BeforeClass;
//
//import java.util.UUID;
//
//public class DefaultMysqlWithSequenceQueueDaoTest extends QueueDaoTest {
//    @BeforeClass
//    public static void beforeClass() {
//        MysqlDatabaseInitializer.initialize();
//    }
//
//    public DefaultMysqlWithSequenceQueueDaoTest() {
//        super(new MssqlQueueDao(MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.DEFAULT_SCHEMA),
//                MysqlDatabaseInitializer.DEFAULT_TABLE_NAME_WO_IDENT, MysqlDatabaseInitializer.DEFAULT_SCHEMA,
//                MysqlDatabaseInitializer.getJdbcTemplate(), MysqlDatabaseInitializer.getTransactionTemplate());
//    }
//
//    @Override
//    protected QueueLocation generateUniqueLocation() {
//        return QueueLocation.builder().withTableName(tableName)
//                .withQueueId(new QueueId("test-queue-" + UUID.randomUUID()))
//                .withIdSequence("tasks_seq").build();
//    }
//}
