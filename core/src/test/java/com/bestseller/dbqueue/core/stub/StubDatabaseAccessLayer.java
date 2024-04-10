package com.bestseller.dbqueue.core.stub;

import com.bestseller.dbqueue.core.config.DatabaseAccessLayer;
import com.bestseller.dbqueue.core.config.DatabaseDialect;
import com.bestseller.dbqueue.core.config.QueueTableSchema;
import com.bestseller.dbqueue.core.dao.QueueDao;
import com.bestseller.dbqueue.core.dao.QueuePickTaskDao;
import com.bestseller.dbqueue.core.settings.FailureSettings;
import com.bestseller.dbqueue.core.settings.PollSettings;
import com.bestseller.dbqueue.core.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;

public class StubDatabaseAccessLayer implements DatabaseAccessLayer {

    private final QueueDao queueDao;

    public StubDatabaseAccessLayer() {
        this.queueDao = mock(QueueDao.class);
    }

    public StubDatabaseAccessLayer(QueueDao queueDao) {
        this.queueDao = queueDao;
    }

    @Override
    @Nonnull
    public QueueDao getQueueDao() {
        return queueDao;
    }

    @Override
    @Nonnull
    public QueuePickTaskDao createQueuePickTaskDao(@Nonnull QueueLocation queueLocation,
                                                   @Nonnull FailureSettings failureSettings,
                                                   @Nonnull PollSettings pollSettings) {
        return mock(QueuePickTaskDao.class);
    }

    @Override
    public <T> T transact(@Nonnull Supplier<T> supplier) {
        return supplier.get();
    }

    @Override
    public void transact(@Nonnull Runnable runnable) {
        runnable.run();
    }

    @Nonnull
    @Override
    public DatabaseDialect getDatabaseDialect() {
        return DatabaseDialect.POSTGRESQL;
    }

    @Nonnull
    @Override
    public QueueTableSchema getQueueTableSchema() {
        return QueueTableSchema.builder().build();
    }
}
