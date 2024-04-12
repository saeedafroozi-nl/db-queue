package com.bestseller.dbqueue.spring.dao.utils;

import com.bestseller.dbqueue.core.config.QueueTableSchema;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.util.Collections;

public class MysqlDatabaseInitializer {
    public static final String DEFAULT_TABLE_NAME = "queue_default";
    public static final String DEFAULT_TABLE_NAME_WO_IDENT = "queue_default_wo_ident";
    public static final String CUSTOM_TABLE_NAME = "queue_custom";
    public static final QueueTableSchema DEFAULT_SCHEMA = QueueTableSchema.builder().build();

    public static final QueueTableSchema CUSTOM_SCHEMA = QueueTableSchema.builder()
            .withIdField("qid")
            .withQueueNameField("qn")
            .withPayloadField("pl")
            .withCreatedAtField("ct")
            .withNextProcessAtField("pt")
            .withAttemptField("at")
            .withReenqueueAttemptField("rat")
            .withTotalAttemptField("tat")
            .withExtFields(Collections.singletonList("trace"))
            .build();

    private static final String MYSQL_CUSTOM_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  qid INT AUTO_INCREMENT PRIMARY KEY,\n" +
            "  qn VARCHAR(127) NOT NULL,\n" +
            "  pl TEXT,\n" +
            "  ct TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
            "  pt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
            "  at INTEGER NOT NULL DEFAULT 0,\n" +
            "  rat INTEGER NOT NULL DEFAULT 0,\n" +
            "  tat INTEGER NOT NULL DEFAULT 0,\n" +
            "  trace TEXT\n" +
            ") ENGINE=InnoDB;";
    private static final String MYSQL_CUSTOM_TABLE_INDEX_DDL =
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (qn, pt, qid DESC);\n" +
            "\n";

    private static final String MYSQL_DEFAULT_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id INT AUTO_INCREMENT PRIMARY KEY,\n" +
            "  queue_name VARCHAR(127) NOT NULL,\n" +
            "  payload TEXT,\n" +
            "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
            "  next_process_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
            "  attempt INTEGER NOT NULL DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER NOT NULL DEFAULT 0,\n" +
            "  total_attempt INTEGER NOT NULL DEFAULT 0\n" +
            ") ENGINE=InnoDB;";
    private static final String MYSQL_DEFAULT_TABLE_INDEX_DDL =
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (queue_name, next_process_at, id DESC);\n" +
            "\n";

    private static final String MYSQL_DEFAULT_WO_IDENT_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id INT NOT NULL,\n" +
            "  queue_name VARCHAR(127) NOT NULL,\n" +
            "  payload TEXT,\n" +
            "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
            "  next_process_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
            "  attempt INTEGER NOT NULL DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER NOT NULL DEFAULT 0,\n" +
            "  total_attempt INTEGER NOT NULL DEFAULT 0,\n" +
            "  PRIMARY KEY (id)\n" +
            ") ENGINE=InnoDB;";
    private static final String MYSQL_DEFAULT_WO_IDENT_TABLE_INDEX_DDL =
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (queue_name, next_process_at, id DESC);\n" +
            "\n";

    private static JdbcTemplate mysqlJdbcTemplate;
    private static TransactionTemplate mysqlTransactionTemplate;

    public static synchronized void initialize() {
        if (mysqlJdbcTemplate != null) {
            return;
        }

        MySQLContainer<?> containerInstance = new MySQLContainer<>(DockerImageName.parse("mysql:8.0-debian"))
                .withUsername("test")
                .withPassword("test")
                .withDatabaseName("test");
        containerInstance.start();

        String url = containerInstance.getJdbcUrl();
        String username = containerInstance.getUsername();
        String password = containerInstance.getPassword();

        DataSource dataSource = createDataSource(url, username, password);
        mysqlJdbcTemplate = new JdbcTemplate(dataSource);
        mysqlTransactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        mysqlTransactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        mysqlTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

        executeDdl(String.format(MYSQL_DEFAULT_TABLE_DDL, DEFAULT_TABLE_NAME));
        executeDdl(String.format(MYSQL_DEFAULT_TABLE_INDEX_DDL, DEFAULT_TABLE_NAME, DEFAULT_TABLE_NAME));

        executeDdl(String.format(MYSQL_DEFAULT_WO_IDENT_TABLE_DDL, DEFAULT_TABLE_NAME_WO_IDENT));
        executeDdl(String.format(MYSQL_DEFAULT_WO_IDENT_TABLE_INDEX_DDL, DEFAULT_TABLE_NAME_WO_IDENT, DEFAULT_TABLE_NAME_WO_IDENT));

        executeDdl(String.format(MYSQL_CUSTOM_TABLE_DDL, CUSTOM_TABLE_NAME));
        executeDdl(String.format(MYSQL_CUSTOM_TABLE_INDEX_DDL, CUSTOM_TABLE_NAME, CUSTOM_TABLE_NAME));
    }

    private static void executeDdl(String ddl) {
        initialize();
        getTransactionTemplate().execute(status -> {
            getJdbcTemplate().execute(ddl);
            return new Object();
        });
    }

    public static JdbcTemplate getJdbcTemplate() {
        initialize();
        return mysqlJdbcTemplate;
    }

    public static TransactionTemplate getTransactionTemplate() {
        initialize();
        return mysqlTransactionTemplate;
    }

    private static DataSource createDataSource(String url, String username, String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }
}
