package io.cdap.cdap.spi.events;

import javax.annotation.Nullable;

public class StartPipelineEvent implements Event<StartPipelineEventDetails> {

    private final long publishTime;
    private final String version;
    private final String projectName;
    private final StartPipelineEventDetails startPipelineEventDetails;

    public StartPipelineEvent(long publishTime, String version,
                              @Nullable String projectName,
                              StartPipelineEventDetails startPipelineEventDetails) {
        this.publishTime = publishTime;
        this.version = version;
        this.projectName = projectName;
        this.startPipelineEventDetails = startPipelineEventDetails;
    }

    @Override
    public EventType getType() {
        return EventType.PIPELINE_START;
    }

    @Override
    public long getPublishTime() {
        return publishTime;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Nullable
    @Override
    public String getInstanceName() {
        return null;
    }

    @Nullable
    @Override
    public String getProjectName() {
        return projectName;
    }

    @Override
    public StartPipelineEventDetails getEventDetails() {
        return startPipelineEventDetails;
    }
}
