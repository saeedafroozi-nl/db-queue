package com.bestseller.dbqueue.spring.dao;

import com.bestseller.dbqueue.core.config.DatabaseAccessLayer;
import com.bestseller.dbqueue.core.config.DatabaseDialect;
import com.bestseller.dbqueue.core.config.QueueTableSchema;
import com.bestseller.dbqueue.core.dao.QueueDao;
import com.bestseller.dbqueue.core.dao.QueuePickTaskDao;
import com.bestseller.dbqueue.core.settings.FailureSettings;
import com.bestseller.dbqueue.core.settings.PollSettings;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Class for interacting with database via Spring JDBC
 *
 * @author Oleg Kandaurov
 * @since 22.04.2021
 */
public class SpringDatabaseAccessLayer implements DatabaseAccessLayer {

    @Nonnull
    private final JdbcOperations jdbcOperations;
    @Nonnull
    private final TransactionOperations transactionOperations;
    @Nonnull
    private final DatabaseDialect databaseDialect;
    @Nonnull
    private final QueueTableSchema queueTableSchema;
    @Nonnull
    private final QueueDao queueDao;


    /**
     * Constructor
     *
     * @param databaseDialect       Database type (dialect)
     * @param queueTableSchema      Queue table scheme.
     * @param jdbcOperations        Reference to Spring JDBC template.
     * @param transactionOperations Reference to Spring transaction template.
     */
    public SpringDatabaseAccessLayer(@Nonnull DatabaseDialect databaseDialect,
                                     @Nonnull QueueTableSchema queueTableSchema,
                                     @Nonnull JdbcOperations jdbcOperations,
                                     @Nonnull TransactionOperations transactionOperations) {
        this.databaseDialect = requireNonNull(databaseDialect);
        this.queueTableSchema = requireNonNull(queueTableSchema);
        this.jdbcOperations = requireNonNull(jdbcOperations);
        this.transactionOperations = requireNonNull(transactionOperations);
        this.queueDao = createQueueDao(databaseDialect, queueTableSchema, jdbcOperations);
    }

    @Override
    @Nonnull
    public QueueDao getQueueDao() {
        return queueDao;
    }

    private QueueDao createQueueDao(@Nonnull DatabaseDialect databaseDialect,
                                    @Nonnull QueueTableSchema queueTableSchema,
                                    @Nonnull JdbcOperations jdbcOperations) {
        requireNonNull(databaseDialect);
        requireNonNull(jdbcOperations);
        requireNonNull(queueTableSchema);
        return switch (databaseDialect) {
            case POSTGRESQL -> new PostgresQueueDao(jdbcOperations, queueTableSchema);
            case MSSQL -> new MssqlQueueDao(jdbcOperations, queueTableSchema);
            case H2 -> new H2QueueDao(jdbcOperations, queueTableSchema);
            case MYSQL -> new MysqlQueueDao(jdbcOperations, queueTableSchema);
            default -> throw new IllegalArgumentException("unsupported database kind: " + databaseDialect);
        };
    }

    @Override
    @Nonnull
    public QueuePickTaskDao createQueuePickTaskDao(
            @Nonnull QueueLocation queueLocation,
            @Nonnull FailureSettings failureSettings,
            @Nonnull PollSettings pollSettings
    ) {
        requireNonNull(databaseDialect);
        requireNonNull(queueTableSchema);
        requireNonNull(queueLocation);
        requireNonNull(failureSettings);
        requireNonNull(pollSettings);
        return switch (databaseDialect) {
            case POSTGRESQL -> new PostgresQueuePickTaskDao(jdbcOperations, queueTableSchema,
                    queueLocation, failureSettings, pollSettings);
            case MSSQL -> new MssqlQueuePickTaskDao(jdbcOperations, queueTableSchema,
                    queueLocation, failureSettings, pollSettings);
            case H2 -> new H2QueuePickTaskDao(jdbcOperations, queueTableSchema,
                    queueLocation, failureSettings, pollSettings);
            default -> throw new IllegalArgumentException("unsupported database kind: " + databaseDialect);
        };
    }

    @Nonnull
    @Override
    public DatabaseDialect getDatabaseDialect() {
        return databaseDialect;
    }

    @Nonnull
    @Override
    public QueueTableSchema getQueueTableSchema() {
        return queueTableSchema;
    }

    @Override
    public <T> T transact(@Nonnull Supplier<T> supplier) {
        requireNonNull(supplier);
        return transactionOperations.execute(status -> supplier.get());
    }

    @Override
    public void transact(@Nonnull Runnable runnable) {
        requireNonNull(runnable);
        transact(() -> {
            runnable.run();
            return null;
        });
    }


    /**
     * Get reference to Spring JDBC template.
     *
     * @return Reference to Spring JDBC template.
     */
    @Nonnull
    public JdbcOperations getJdbcOperations() {
        return jdbcOperations;
    }

    /**
     * Get reference to Spring transaction template.
     *
     * @return Reference to Spring transaction template.
     */
    @Nonnull
    public TransactionOperations getTransactionOperations() {
        return transactionOperations;
    }
}
