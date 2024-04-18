package com.bestseller.dbqueue.spring.dao;

import com.bestseller.dbqueue.core.api.EnqueueParams;
import com.bestseller.dbqueue.core.config.QueueTableSchema;
import com.bestseller.dbqueue.core.dao.QueueDao;
import com.bestseller.dbqueue.core.settings.QueueLocation;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class MysqlQueueDao implements QueueDao {

    private final Map<QueueLocation, String> enqueueSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> deleteSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> reenqueueSqlCache = new ConcurrentHashMap<>();

    @Nonnull
    private final NamedParameterJdbcTemplate jdbcTemplate;
    @Nonnull
    private final QueueTableSchema queueTableSchema;

    /**
     * Constructor
     *
     * @param jdbcTemplate     Reference to Spring JDBC template.
     * @param queueTableSchema Queue table scheme.
     */
    public MysqlQueueDao(@Nonnull JdbcOperations jdbcTemplate,
                         @Nonnull QueueTableSchema queueTableSchema) {
        this.queueTableSchema = requireNonNull(queueTableSchema);
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(jdbcTemplate));
    }

    @Override
    public long enqueue(@Nonnull QueueLocation location, @Nonnull EnqueueParams<String> enqueueParams) {
        requireNonNull(location);
        requireNonNull(enqueueParams);
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("queueName", location.getQueueId().asString())
                .addValue("payload", enqueueParams.getPayload())
                .addValue("executionDelay", enqueueParams.getExecutionDelay().getSeconds());

        queueTableSchema.getExtFields().forEach(paramName -> params.addValue(paramName, null));
        enqueueParams.getExtData().forEach(params::addValue);
        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder(); // Create KeyHolder to hold generated keys
        try {
            String sql = enqueueSqlCache.computeIfAbsent(location, this::createEnqueueSql);
            jdbcTemplate.update(sql, params, keyHolder); // Pass KeyHolder to the update method
            Number generatedId = keyHolder.getKey(); // Retrieve the generated ID
            if (generatedId != null) {
                return requireNonNull(generatedId.longValue()); // Return the generated ID
            } else {
                throw new RuntimeException("Failed to retrieve generated ID after insert operation");
            }
        }catch (DataAccessException e) {
            e.printStackTrace();
            return -1; //
        }
    }

    @Override
    public void enqueueBatch(@Nonnull QueueLocation location, @Nonnull List<EnqueueParams<String>> enqueueParams) {
        throw new UnsupportedOperationException("batch enqueue is not supported for this type of database");
    }

    @Override
    public boolean deleteTask(@Nonnull QueueLocation location, long taskId) {
        requireNonNull(location);
        int updatedRows = jdbcTemplate.update(deleteSqlCache.computeIfAbsent(location, this::createDeleteSql),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString()));
        return updatedRows != 0;
    }

    @Override
    public boolean reenqueue(@Nonnull QueueLocation location, long taskId, @Nonnull Duration executionDelay) {
        requireNonNull(location);
        requireNonNull(executionDelay);
        int updatedRows = jdbcTemplate.update(reenqueueSqlCache.computeIfAbsent(location, this::createReenqueueSql),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString())
                        .addValue("executionDelay", executionDelay.getSeconds()));
        return updatedRows != 0;
    }

    private String createEnqueueSql(@Nonnull QueueLocation location) {
        return "INSERT INTO " + location.getTableName() + "(" +
                queueTableSchema.getQueueNameField() + "," +
                queueTableSchema.getPayloadField() + "," +
                queueTableSchema.getNextProcessAtField() + "," +
                queueTableSchema.getReenqueueAttemptField() + "," +
                queueTableSchema.getTotalAttemptField() +
                (queueTableSchema.getExtFields().isEmpty() ? "" :
                        queueTableSchema.getExtFields().stream().collect(Collectors.joining(", ", ", ", ""))) +
                ") VALUES (" +
                ":queueName, :payload,DATE_ADD(CURRENT_TIMESTAMP(),INTERVAL :executionDelay SECOND), " +
                "0, 0" +
                (queueTableSchema.getExtFields().isEmpty() ? "" : queueTableSchema.getExtFields().stream()
                        .map(field -> ":" + field).collect(Collectors.joining(", ", ", ", ""))) +
                ")";
    }


    private String createDeleteSql(@Nonnull QueueLocation location) {
        return "DELETE FROM " + location.getTableName() + " WHERE " + queueTableSchema.getQueueNameField() +
                " = :queueName AND " + queueTableSchema.getIdField() + " = :id";
    }

    private String createReenqueueSql(@Nonnull QueueLocation location) {
        return "UPDATE " + location.getTableName() + " SET " +
                queueTableSchema.getNextProcessAtField() + " = DATE_ADD(CURRENT_TIMESTAMP(),INTERVAL :executionDelay " +
                "SECOND), " +
                queueTableSchema.getAttemptField() + " = 0, " +
                queueTableSchema.getReenqueueAttemptField() + " = " +
                queueTableSchema.getReenqueueAttemptField() + " + 1 " +
                "WHERE " + queueTableSchema.getIdField() + " = :id AND " +
                queueTableSchema.getQueueNameField() + " = :queueName";
    }

}
