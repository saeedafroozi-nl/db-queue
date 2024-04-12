package com.bestseller.dbqueue.spring.dao;

import com.bestseller.dbqueue.core.api.TaskRecord;
import com.bestseller.dbqueue.core.config.QueueTableSchema;
import com.bestseller.dbqueue.core.dao.QueuePickTaskDao;
import com.bestseller.dbqueue.core.settings.FailRetryType;
import com.bestseller.dbqueue.core.settings.FailureSettings;
import com.bestseller.dbqueue.core.settings.PollSettings;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;

public class MysqlQueuePickTaskDao implements QueuePickTaskDao {

    private static final Logger log = LoggerFactory.getLogger(MysqlQueuePickTaskDao.class);

    private String pickTaskSql;
    private MapSqlParameterSource pickTaskSqlPlaceholders;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final QueueTableSchema queueTableSchema;

    public MysqlQueuePickTaskDao(@Nonnull JdbcOperations jdbcTemplate,
                                 @Nonnull QueueTableSchema queueTableSchema,
                                 @Nonnull QueueLocation queueLocation,
                                 @Nonnull FailureSettings failureSettings,
                                 @Nonnull PollSettings pollSettings) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(jdbcTemplate));
        this.queueTableSchema = requireNonNull(queueTableSchema);
        pickTaskSqlPlaceholders = new MapSqlParameterSource()
                .addValue("queueName", queueLocation.getQueueId().asString())
                .addValue("retryInterval", failureSettings.getRetryInterval().toMillis());
        pickTaskSql = createPickTaskSql(queueLocation, failureSettings);
        failureSettings.registerObserver((oldValue, newValue) -> {
            pickTaskSql = createPickTaskSql(queueLocation, newValue);
            pickTaskSqlPlaceholders = new MapSqlParameterSource()
                    .addValue("queueName", queueLocation.getQueueId().asString())
                    .addValue("retryInterval", newValue.getRetryInterval().toMillis());
        });
        pollSettings.registerObserver((oldValue, newValue) -> {
            if (newValue.getBatchSize() != 1) {
                log.warn("Cannot set batchSize. Size other than one is not supported, ignoring.");
            }
        });
    }

    @Override
    @Nonnull
    public List<TaskRecord> pickTasks() {
        return requireNonNull(jdbcTemplate.execute(pickTaskSql,
                pickTaskSqlPlaceholders,
                (PreparedStatement ps) -> {
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            //noinspection ReturnOfNull
                            return List.of();
                        }

                        Map<String, String> additionalData = new LinkedHashMap<>();
                        queueTableSchema.getExtFields().forEach(key -> {
                            try {
                                additionalData.put(key, rs.getString(key));
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
                        return List.of(TaskRecord.builder()
                                .withId(rs.getLong(queueTableSchema.getIdField()))
                                .withCreatedAt(getZonedDateTime(rs, queueTableSchema.getCreatedAtField()))
                                .withNextProcessAt(getZonedDateTime(rs, queueTableSchema.getNextProcessAtField()))
                                .withPayload(rs.getString(queueTableSchema.getPayloadField()))
                                .withAttemptsCount(rs.getLong(queueTableSchema.getAttemptField()))
                                .withReenqueueAttemptsCount(rs.getLong(queueTableSchema.getReenqueueAttemptField()))
                                .withTotalAttemptsCount(rs.getLong(queueTableSchema.getTotalAttemptField()))
                                .withExtData(additionalData).build());
                    }
                }));
    }

    private String createPickTaskSql(@Nonnull QueueLocation location, FailureSettings failureSettings) {
        return "UPDATE " + location.getTableName() + " AS t1, " +
                "(SELECT " + queueTableSchema.getIdField() + " " +
                "FROM " + location.getTableName() +
                " WHERE " + queueTableSchema.getQueueNameField() + " = :queueName " +
                "AND " + queueTableSchema.getNextProcessAtField() + " <= NOW() " +
                "ORDER BY " + queueTableSchema.getNextProcessAtField() + " ASC " +
                "LIMIT 1) AS t2 " +
                "SET t1." + queueTableSchema.getNextProcessAtField() + " = " +
                getNextProcessTimeSql(failureSettings.getRetryType(), queueTableSchema) + ", " +
                "t1." + queueTableSchema.getAttemptField() + " = t1." + queueTableSchema.getAttemptField() + " + 1, " +
                "t1." + queueTableSchema.getTotalAttemptField() + " = t1." + queueTableSchema.getTotalAttemptField() + " + 1 " +
                "WHERE t1." + queueTableSchema.getIdField() + " = t2." + queueTableSchema.getIdField();
    }

    private ZonedDateTime getZonedDateTime(ResultSet rs, String time) throws SQLException {
        return ZonedDateTime.ofInstant(rs.getTimestamp(time).toInstant(), ZoneId.systemDefault());
    }


    @Nonnull
    private String getNextProcessTimeSql(@Nonnull FailRetryType failRetryType, QueueTableSchema queueTableSchema) {
        requireNonNull(failRetryType);
        return switch (failRetryType) {
            case GEOMETRIC_BACKOFF ->
                    "DATE_ADD(NOW(), INTERVAL POW(2, " + queueTableSchema.getAttemptField() + ") * :retryInterval / 1000 SECOND)";
            case ARITHMETIC_BACKOFF ->
                    "DATE_ADD(NOW(), INTERVAL (1 + (" + queueTableSchema.getAttemptField() + " * 2)) * :retryInterval / 1000 SECOND)";
            case LINEAR_BACKOFF -> "DATE_ADD(NOW(), INTERVAL :retryInterval / 1000 SECOND)";
        };
    }
}
