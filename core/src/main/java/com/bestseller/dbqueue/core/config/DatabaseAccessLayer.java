package com.bestseller.dbqueue.core.config;

import com.bestseller.dbqueue.core.dao.QueueDao;
import com.bestseller.dbqueue.core.dao.QueuePickTaskDao;
import com.bestseller.dbqueue.core.settings.FailureSettings;
import com.bestseller.dbqueue.core.settings.PollSettings;
import com.bestseller.dbqueue.core.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * Interface for interacting with database
 *
 * @author Oleg Kandaurov
 * @since 22.04.2021
 */
public interface DatabaseAccessLayer {

    /**
     * Get an instance of database-specific DAO based on database type and table schema.
     *
     * @return database-specific DAO instance.
     */
    @Nonnull
    QueueDao getQueueDao();

    /**
     * Create an instance of database-specific DAO based on database type and table schema.
     *
     * @param queueLocation   queue location
     * @param failureSettings settings for handling failures
     * @param pollSettings    settings for polling
     * @return database-specific DAO instance.
     */
    @Nonnull
    QueuePickTaskDao createQueuePickTaskDao(
            @Nonnull QueueLocation queueLocation,
            @Nonnull FailureSettings failureSettings,
            @Nonnull PollSettings pollSettings
    );

    /**
     * Perform an operation in transaction
     *
     * @param <ResultT> result type
     * @param supplier  operation
     * @return result of operation
     */
    <ResultT> ResultT transact(@Nonnull Supplier<ResultT> supplier);

    /**
     * Perform an operation in transaction
     *
     * @param runnable operation
     */
    void transact(@Nonnull Runnable runnable);

    /**
     * Get database type for that database.
     *
     * @return Database type.
     */
    @Nonnull
    DatabaseDialect getDatabaseDialect();

    /**
     * Get queue table schema for that database.
     *
     * @return Queue table schema.
     */
    @Nonnull
    QueueTableSchema getQueueTableSchema();

}
