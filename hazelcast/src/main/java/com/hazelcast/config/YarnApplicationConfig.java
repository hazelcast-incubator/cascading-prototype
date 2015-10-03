package com.hazelcast.config;

import static com.hazelcast.util.Preconditions.checkTrue;

public class YarnApplicationConfig {
    public static final int DEFAULT_APP_ATTEMPTS_COUNT = 100;

    public static final int DEFAULT_FILE_CHUNK_SIZE_BYTES = 1024;

    public static final String DEFAULT_LOCALIZATION_TYPE = "MEMORY";

    public static final String DEFAULT_LOCALIZATION_DIRECTORY = System.getProperty("java.io.tmpdir");

    public static final int DEFAULT_YARN_SECONDS_TO_AWAIT = 30;

    public static final long DEFAULT_DAG_PROCESSING_SECONDS_TO_AWAIT = 30L;

    public static final long DEFAULT_EXECUTION_PROCESSING_SECONDS_TO_AWAIT = 30L;

    private static final int DEFAULT_APPLICATION_INVOCATION_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();

    private static final int DEFAULT_QUEUE_SIZE = 65536;

    private static final int DEFAULT_STREAM_CHUNK_SIZE = 256;

    private static final int DEFAULT_SHUFFLING_BATCH_SIZE_BYTES = 256;

    private static final int DEFAULT_MAX_SHUFFLING_QUEUE_FAILURES_ATTEMPTS = 10;

    private String localizationDirectory = DEFAULT_LOCALIZATION_DIRECTORY;

    private int resourceFileChunkSize = DEFAULT_FILE_CHUNK_SIZE_BYTES;

    private int maxShufflingQueueFailuresAttempts = DEFAULT_MAX_SHUFFLING_QUEUE_FAILURES_ATTEMPTS;

    private String localizationType = DEFAULT_LOCALIZATION_TYPE;

    private int defaultApplicationDirectoryCreationAttemptsCount = DEFAULT_APP_ATTEMPTS_COUNT;

    private int yarnSecondsToWwait = DEFAULT_YARN_SECONDS_TO_AWAIT;

    private long applicationSecondsToAwait = DEFAULT_DAG_PROCESSING_SECONDS_TO_AWAIT;

    private long executionProcessingSecondsToAwait = DEFAULT_EXECUTION_PROCESSING_SECONDS_TO_AWAIT;

    private int applicationInvocationThreadPoolSize = DEFAULT_APPLICATION_INVOCATION_THREAD_POOL_SIZE;

    private int containerQueueSize = DEFAULT_QUEUE_SIZE;

    private int tupleChunkSize = DEFAULT_STREAM_CHUNK_SIZE;

    private int maxProcessingThreads = -1;

    public void setShufflingBatchSizeBytes(int shufflingBatchSizeBytes) {
        this.shufflingBatchSizeBytes = shufflingBatchSizeBytes;
    }

    private int shufflingBatchSizeBytes = DEFAULT_SHUFFLING_BATCH_SIZE_BYTES;

    private PartitioningStrategyConfig partitioningStrategyConfig = new PartitioningStrategyConfig();

    private final YarnApplicationConfig defConfig;
    private final String name;

    public YarnApplicationConfig(YarnApplicationConfig defConfig, String name) {
        this.name = name;
        this.defConfig = defConfig;

    }

    public YarnApplicationConfig(String name) {
        this.defConfig = null;
        this.name = name;
    }

    public YarnApplicationConfig getAsReadOnly() {
        return new YarnApplicationConfigReadOnly(this, name);
    }

    public String getName() {
        return this.name;
    }

    public String getLocalizationType() {
        return localizationType;
    }

    public void setLocalizationType(String localizationType) {
        this.localizationType = localizationType;
    }

    public int getResourceFileChunkSize() {
        return resourceFileChunkSize;
    }

    public void setResourceFileChunkSize(byte resourceFileChunkSize) {
        this.resourceFileChunkSize = resourceFileChunkSize;
    }

    public String getLocalizationDirectory() {
        return localizationDirectory;
    }

    public void setLocalizationDirectory(String localizationDirectory) {
        this.localizationDirectory = localizationDirectory;
    }

    public int getDefaultApplicationDirectoryCreationAttemptsCount() {
        return defaultApplicationDirectoryCreationAttemptsCount;
    }

    public void setDefaultApplicationDirectoryCreationAttemptsCount(int defaultApplicationDirectoryCreationAttemptsCount) {
        this.defaultApplicationDirectoryCreationAttemptsCount = defaultApplicationDirectoryCreationAttemptsCount;
    }

    public void setApplicationSecondsToAwait(int applicationSecondsToAwait) {
        this.applicationSecondsToAwait = applicationSecondsToAwait;
    }

    public void setYarnSecondsToAwait(int yarnSecondsToAwait) {
        this.yarnSecondsToWwait = yarnSecondsToAwait;
    }

    public PartitioningStrategyConfig getPartitioningStrategyConfig() {
        return partitioningStrategyConfig;
    }

    public String getDefaultLocalizationDirectory() {
        return DEFAULT_LOCALIZATION_DIRECTORY;
    }

    public int getYarnSecondsToAwait() {
        return yarnSecondsToWwait;
    }

    public long getApplicationSecondsToAwait() {
        return applicationSecondsToAwait;
    }

    public long getExecutionProcessingSecondsToAwait() {
        return executionProcessingSecondsToAwait;
    }

    public int getApplicationInvocationThreadPoolSize() {
        return applicationInvocationThreadPoolSize;
    }

    public int getContainerQueueSize() {
        return containerQueueSize;
    }

    public void setContainerQueueSize(int containerQueueSize) {
        checkTrue(Integer.bitCount(containerQueueSize) == 1, "containerQueueSize should be power of 2");
        this.containerQueueSize = containerQueueSize;
    }

    public int getTupleChunkSize() {
        return tupleChunkSize;
    }

    public void setTupleChunkSize(int tupleChunkSize) {
        this.tupleChunkSize = tupleChunkSize;
    }

    public int getMaxProcessingThreads() {
        return maxProcessingThreads <= 0 ? Runtime.getRuntime().availableProcessors() : maxProcessingThreads;
    }

    public void setMaxProcessingThreads(int maxProcessingThreads) {
        this.maxProcessingThreads = maxProcessingThreads;
    }

    public int getShufflingBatchSizeBytes() {
        return shufflingBatchSizeBytes;
    }

    public int getMaxShufflingQueueFailuresAttempts() {
        return maxShufflingQueueFailuresAttempts;
    }

    public void setMaxShufflingQueueFailuresAttempts(int maxShufflingQueueFailuresAttempts) {
        this.maxShufflingQueueFailuresAttempts = maxShufflingQueueFailuresAttempts;
    }
}
