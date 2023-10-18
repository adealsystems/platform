package org.adealsystems.platform.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TestFileBasedEventHistory extends FileBasedEventHistory {

    private final List<InternalEvent> consumedEvents = new ArrayList<>();

    public TestFileBasedEventHistory(File baseDirectory, ObjectMapper objectMapper) {
        super(baseDirectory, new FileBasedRunRepository(new File(".")), objectMapper);
    }

    public TestFileBasedEventHistory(File baseDirectory, TimestampFactory timestampFactory, RunRepository runRepository, ObjectMapper objectMapper) {
        super(baseDirectory, timestampFactory, runRepository, objectMapper);
    }

    public List<InternalEvent> getConsumedEvents() {
        return consumedEvents;
    }

    @Override
    public void add(InternalEvent event) {
        Objects.requireNonNull(event, "event must not be null!");
        addAll(Collections.singleton(event));
    }

    @Override
    public void addAll(Iterable<InternalEvent> events) {
        for (InternalEvent event : events) {
            consumedEvents.add(event);
        }

        super.addAll(events);
    }
}
