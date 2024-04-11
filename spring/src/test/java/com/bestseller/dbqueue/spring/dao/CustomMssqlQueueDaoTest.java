//package com.bestseller.dbqueue.spring.dao;
//
//import com.bestseller.dbqueue.spring.dao.utils.MssqlDatabaseInitializer;
//import org.junit.BeforeClass;
//
//
///**
// * @author Oleg Kandaurov
// * @author Behrooz Shabani
// * @since 25.01.2020
// */
//public class CustomMssqlQueueDaoTest extends QueueDaoTest {
//
//    @BeforeClass
//    public static void beforeClass() {
//        MssqlDatabaseInitializer.initialize();
//    }
//
//    public CustomMssqlQueueDaoTest() {
//        super(new MssqlQueueDao(MssqlDatabaseInitializer.getJdbcTemplate(), MssqlDatabaseInitializer.CUSTOM_SCHEMA),
//                MssqlDatabaseInitializer.CUSTOM_TABLE_NAME, MssqlDatabaseInitializer.CUSTOM_SCHEMA,
//                MssqlDatabaseInitializer.getJdbcTemplate(), MssqlDatabaseInitializer.getTransactionTemplate());
//    }
//}
