package com.bestseller.dbqueue.core.stub;

import com.bestseller.dbqueue.core.settings.ExtSettings;
import com.bestseller.dbqueue.core.settings.FailRetryType;
import com.bestseller.dbqueue.core.settings.FailureSettings;
import com.bestseller.dbqueue.core.settings.PollSettings;
import com.bestseller.dbqueue.core.settings.ProcessingMode;
import com.bestseller.dbqueue.core.settings.ProcessingSettings;
import com.bestseller.dbqueue.core.settings.QueueSettings;
import com.bestseller.dbqueue.core.settings.ReenqueueRetryType;
import com.bestseller.dbqueue.core.settings.ReenqueueSettings;

import java.time.Duration;
import java.util.HashMap;

public class TestFixtures {

    public static QueueSettings.Builder createQueueSettings() {
        return QueueSettings.builder()
                .withProcessingSettings(createProcessingSettings().build())
                .withPollSettings(createPollSettings().build())
                .withFailureSettings(createFailureSettings().build())
                .withReenqueueSettings(createReenqueueSettings().build())
                .withExtSettings(ExtSettings.builder().withSettings(new HashMap<>()).build());
    }

    public static ProcessingSettings.Builder createProcessingSettings() {
        return ProcessingSettings.builder()
                .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS)
                .withThreadCount(1);
    }

    public static PollSettings.Builder createPollSettings() {
        return PollSettings.builder()
                .withBetweenTaskTimeout(Duration.ofMillis(0))
                .withNoTaskTimeout(Duration.ofMillis(0))
                .withFatalCrashTimeout(Duration.ofSeconds(0))
                .withBatchSize(1)
                .withQueryVersion(0);
    }

    public static FailureSettings.Builder createFailureSettings() {
        return FailureSettings.builder()
                .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
                .withRetryInterval(Duration.ofMinutes(1));
    }

    public static ReenqueueSettings.Builder createReenqueueSettings() {
        return ReenqueueSettings.builder()
                .withRetryType(ReenqueueRetryType.MANUAL);
    }
}
