//package com.bestseller.dbqueue.core.settings;
//
//import com.bestseller.dbqueue.core.config.QueueService;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.nio.file.StandardOpenOption;
//import java.time.Duration;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.List;
//
//import static java.util.concurrent.TimeUnit.SECONDS;
//import static org.mockito.ArgumentMatchers.eq;
//import static org.mockito.Mockito.atLeast;
//import static org.mockito.Mockito.atLeastOnce;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.spy;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.verifyNoInteractions;
//import static org.awaitility.Awaitility.await;
//import static org.assertj.core.api.Assertions.assertThat;
//
//public class QueueConfigsReloaderTest {
//
//    private static Path tempDir;
//
//    @BeforeClass
//    public static void beforeClass() throws Exception {
//        tempDir = Files.createTempDirectory(Paths.get(System.getProperty("user.dir"), "target"),
//                QueueConfigsReloaderTest.class.getSimpleName());
//        tempDir.toFile().deleteOnExit();
//    }
//
//    Path write(String... lines) throws IOException {
//        Path tempFile = Files.createTempFile(tempDir, "queue", ".properties");
//        Files.write(tempFile, Arrays.asList(lines), StandardOpenOption.WRITE, StandardOpenOption.APPEND);
//        tempFile.toFile().deleteOnExit();
//        return tempFile;
//    }
//
//    private QueueConfigsReader createReader(List<Path> paths) {
//        return new QueueConfigsReader(paths, "q",
//                () -> ProcessingSettings.builder()
//                        .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS),
//                () -> PollSettings.builder()
//                        .withBetweenTaskTimeout(Duration.ofSeconds(9))
//                        .withNoTaskTimeout(Duration.ofSeconds(99))
//                        .withFatalCrashTimeout(Duration.ofSeconds(999))
//                        .withBatchSize(1)
//                        .withQueryVersion(0),
//                () -> FailureSettings.builder()
//                        .withRetryInterval(Duration.ofMinutes(9)).withRetryType(FailRetryType.GEOMETRIC_BACKOFF),
//                () -> ReenqueueSettings.builder()
//                        .withRetryType(ReenqueueRetryType.MANUAL));
//    }
//
//    @Test
//    public void should_not_reload_bad_configs() throws Exception {
//        Path configPath = write("");
//
//        QueueService queueService = mock(QueueService.class);
//        QueueConfigsReader reader = spy(createReader(Collections.singletonList(configPath)));
//
//        QueueConfigsReloader reloader = new QueueConfigsReloader(reader, queueService);
//        reloader.start();
//        Files.write(configPath, "q.testname.table=foo".getBytes(StandardCharsets.UTF_8));
//        Thread.sleep(1000);
//        reloader.stop();
//        await().atMost(20, SECONDS).untilAsserted(() -> {
//            verify(reader, atLeastOnce()).parse();
//            verifyNoInteractions(queueService);
//        });
//        verifyNoInteractions(queueService);
//    }
//
//    @Test
//    public void should_reload_correct_configs() throws Exception {
//        Path configPath = write("");
//
//        QueueService queueService = mock(QueueService.class);
//        QueueConfigsReader reader = spy(createReader(Collections.singletonList(configPath)));
//
//        QueueConfigsReloader reloader = new QueueConfigsReloader(reader, queueService);
//        reloader.start();
//        Files.write(configPath, ("q.testname.table=foo" + System.lineSeparator() + "q.testname.thread-count=1")
//                .getBytes(StandardCharsets.UTF_8));
//        List<QueueConfig> queueConfigs = reader.parse();
//        Thread.sleep(1000);
//        reloader.stop();
//        await().atMost(20, SECONDS).untilAsserted(() -> {
//        verify(reader, atLeast(2)).parse();
//        verify(queueService, atLeastOnce()).updateQueueConfigs(eq(queueConfigs));
//        });
//    }
//}