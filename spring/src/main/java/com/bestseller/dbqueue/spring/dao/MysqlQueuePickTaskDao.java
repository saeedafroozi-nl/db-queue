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
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static java.util.Objects.requireNonNull;

public class MysqlQueuePickTaskDao implements QueuePickTaskDao {

    private static final Logger log = LoggerFactory.getLogger(MysqlQueuePickTaskDao.class);

    private String updateTaskSql;
    private String selectUpdatingTaskSql;
    private String selectUpdatedTaskSql;
    private MapSqlParameterSource updateTaskSqlPlaceholders;
    private MapSqlParameterSource selectUpdatingTaskSqlPlaceholders;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final QueueTableSchema queueTableSchema;

    public MysqlQueuePickTaskDao(@Nonnull JdbcOperations jdbcTemplate,
                                 @Nonnull QueueTableSchema queueTableSchema,
                                 @Nonnull QueueLocation queueLocation,
                                 @Nonnull FailureSettings failureSettings,
                                 @Nonnull PollSettings pollSettings) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(jdbcTemplate));
        this.queueTableSchema = requireNonNull(queueTableSchema);

        updateTaskSqlPlaceholders = new MapSqlParameterSource()
                .addValue("queueName", queueLocation.getQueueId().asString())
                .addValue("retryInterval", failureSettings.getRetryInterval().getSeconds());

        updateTaskSql = createPickTaskSql(queueLocation, failureSettings);

        selectUpdatingTaskSql = createSelectPickTaskSql(queueLocation);

        selectUpdatedTaskSql = "SELECT * FROM " + queueLocation.getTableName() + " WHERE " + queueTableSchema.getIdField() + " = :id";

        selectUpdatingTaskSqlPlaceholders = new MapSqlParameterSource()
                .addValue("queueName", queueLocation.getQueueId().asString());

        failureSettings.registerObserver((oldValue, newValue) -> {
            updateTaskSql = createPickTaskSql(queueLocation, newValue);

            updateTaskSqlPlaceholders = new MapSqlParameterSource()
                    .addValue("queueName", queueLocation.getQueueId().asString())
                    .addValue("retryInterval", newValue.getRetryInterval().getSeconds());

            selectUpdatingTaskSql = createSelectPickTaskSql(queueLocation);

            selectUpdatingTaskSqlPlaceholders = new MapSqlParameterSource().addValue("queueName", queueLocation.getQueueId().asString());

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

        List<Long> ids = jdbcTemplate.query(selectUpdatingTaskSql, selectUpdatingTaskSqlPlaceholders,
                (resultSet, rowNum) -> resultSet.getLong(queueTableSchema.getIdField()));

        if (!ids.isEmpty()) {
            Long id = ids.get(0);
            // Execute the UPDATE query
            updateTaskSqlPlaceholders.addValue("id", id);
            jdbcTemplate.update(updateTaskSql, updateTaskSqlPlaceholders);
            // Return the updated record
            return buildTaskRecordFromResultSet();
        } else {
            // No row to update
            return List.of();
        }
    }

    private List<TaskRecord> buildTaskRecordFromResultSet() {
        return requireNonNull(jdbcTemplate.execute(selectUpdatedTaskSql, updateTaskSqlPlaceholders,
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

    private String createSelectPickTaskSql(@Nonnull QueueLocation location) {
        return "SELECT " + queueTableSchema.getIdField() + " FROM " + location.getTableName() + " WHERE " + queueTableSchema.getQueueNameField() +
                " = :queueName AND " + queueTableSchema.getNextProcessAtField() + " <= CURRENT_TIMESTAMP() " +
                "ORDER BY " + queueTableSchema.getNextProcessAtField() + " ASC LIMIT 1";
    }

    private String createPickTaskSql(@Nonnull QueueLocation location, FailureSettings failureSettings) {
        return "UPDATE " + location.getTableName() + " SET " +
                queueTableSchema.getNextProcessAtField() + " = " + getNextProcessTimeSql(failureSettings.getRetryType(),
                queueTableSchema) + ", " +
                queueTableSchema.getAttemptField() + " = " + queueTableSchema.getAttemptField() + " + 1, " +
                queueTableSchema.getTotalAttemptField() + " = " + queueTableSchema.getTotalAttemptField() + " + 1 " +
                "WHERE " + queueTableSchema.getIdField() + " = :id";

    }

    private ZonedDateTime getZonedDateTime(ResultSet rs, String fieldName) throws SQLException {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        Timestamp ts = rs.getTimestamp(fieldName, cal);
        return ZonedDateTime.ofInstant(ts.toInstant(),
                ZoneId.systemDefault());
    }

    @Nonnull
    private String getNextProcessTimeSql(@Nonnull FailRetryType failRetryType, QueueTableSchema queueTableSchema) {
        requireNonNull(failRetryType);
        requireNonNull(failRetryType);
        return switch (failRetryType) {
            case GEOMETRIC_BACKOFF ->
                    "DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL POW(2, " + queueTableSchema.getAttemptField() + ") * " + ":retryInterval SECOND) ";
            case ARITHMETIC_BACKOFF ->
                    "DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL (1 + (" + queueTableSchema.getAttemptField() + " * 2)) * " + ":retryInterval SECOND)";
            case LINEAR_BACKOFF -> "DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL :retryInterval SECOND)";
        };
    }
}
