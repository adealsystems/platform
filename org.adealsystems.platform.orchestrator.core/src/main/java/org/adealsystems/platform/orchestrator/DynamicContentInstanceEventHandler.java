/*
 * Copyright 2020-2024 ADEAL Systems GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.adealsystems.platform.orchestrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.adealsystems.platform.orchestrator.SessionEventConstants.DYNAMIC_CONTENT_ATTRIBUTE_NAME;


public final class DynamicContentInstanceEventHandler implements InternalEventClassifier, InstanceEventHandler, SessionInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicContentInstanceEventHandler.class);

    private static final ConcurrentMap<Class<? extends DynamicContentAwareHandler>, DynamicContentInstanceEventHandler> HANDLERS = new ConcurrentHashMap<>();
    private static final String PROTOTYPE_HANDLER_KEY = "_prototype_handler";

    private final Map<String, DynamicContentAwareHandler> dynamicHandlers = new HashMap<>();
    private final ApplicationContext applicationContext;
    private final Class<? extends DynamicContentAwareHandler> handlerClass;

    public static DynamicContentInstanceEventHandler forHandler(
        ApplicationContext applicationContext,
        Class<? extends DynamicContentAwareHandler> handlerClass
    ) {
        DynamicContentInstanceEventHandler result = HANDLERS.get(handlerClass);
        if (result == null) {
            result = new DynamicContentInstanceEventHandler(applicationContext, handlerClass);
            HANDLERS.put(handlerClass, result);
        }

        return result;
    }

    private DynamicContentInstanceEventHandler(
        ApplicationContext applicationContext,
        Class<? extends DynamicContentAwareHandler> handlerClass
    ) {
        this.applicationContext = Objects.requireNonNull(applicationContext, "applicationContext must not be null!");
        this.handlerClass = Objects.requireNonNull(handlerClass, "handlerClass must not be null!");
    }

    @Override
    public Optional<RunSpecification> getCurrentRun() {
        DynamicContentAwareHandler handler = resolvePrototypeHandler();
        return handler.getCurrentRun();
    }

    @Override
    public boolean isValid(InternalEvent event) {
        // No dynamic attribute available in the event
        DynamicContentAwareHandler handler = resolvePrototypeHandler();
        return handler.isValid(event);
    }

    @Override
    public void initializeSession(Session session) {
        Optional<String> oDynamicContent = session.getStateValue(DYNAMIC_CONTENT_ATTRIBUTE_NAME);
        if (!oDynamicContent.isPresent()) {
            LOGGER.warn("Session {} does not own dynamic content!", session);
            return;
        }

        String dynamicContent = oDynamicContent.get();
        DynamicContentAwareHandler handler = dynamicHandlers.get(dynamicContent);
        if (handler == null) {
            LOGGER.warn("Missing dynamic handler for {}!", dynamicContent);
            return;
        }

        if (SessionInitializer.class.isAssignableFrom(handler.getClass())) {
            ((SessionInitializer) handler).initializeSession(session);
            LOGGER.info("Initialized session: {}, processingState: {}", session, session.getProcessingState());
        }
    }

    @Override
    public boolean isRelevant(InternalEvent event) {
        // No dynamic attribute available in the event
        DynamicContentAwareHandler handler = resolvePrototypeHandler();
        LOGGER.debug("Check relevance of '{}'", event.getId());
        return handler.isRelevant(event);
    }

    @Override
    public boolean isSessionStartEvent(InternalEvent event) {
        DynamicContentAwareHandler handler = resolveDynamicHandler(event);
        if (handler == null) {
            LOGGER.debug("No dynamic handler found for {}", event);
            return false;
        }

        return handler.isSessionStartEvent(event);
    }

    @Override
    public boolean isSessionStopEvent(InternalEvent event, Session session) {
        DynamicContentAwareHandler handler = resolveDynamicHandler(event);
        if (handler == null) {
            throw new MissingDynamicHandlerException(event);
        }

        boolean isStopEvent = handler.isSessionStopEvent(event, session);
        cleanupDynamicContentHandler(event, handler);
        return isStopEvent;
    }

    @Override
    public boolean isTerminating(InternalEvent event) {
        DynamicContentAwareHandler handler = resolveDynamicHandler(event);
        if (handler == null) {
            throw new MissingDynamicHandlerException(event);
        }
        return handler.isTerminating(event);
    }

    @Override
    public void resetTerminatingFlag(InternalEvent event) {
        DynamicContentAwareHandler handler = resolveDynamicHandler(event);
        if (handler == null) {
            throw new MissingDynamicHandlerException(event);
        }
        handler.resetTerminatingFlag(event);
    }

    @Override
    public Optional<String> determineDynamicContent(InternalEvent event) {
        // No dynamic attribute available in the event
        DynamicContentAwareHandler handler = resolvePrototypeHandler();
        return handler.determineDynamicContent(event);
    }

    @Override
    public InternalEvent handle(InternalEvent event, Session session) {
        InstanceEventHandler handler = resolveDynamicHandler(event);

        if (handler == null) {
            LOGGER.debug("No dynamic content handler found for event {}", event);
            return event;
        }

        return handler.handle(event, session);
    }

    private DynamicContentAwareHandler resolvePrototypeHandler() {
        LOGGER.debug("resolving prototype handler for {}", handlerClass);
        DynamicContentAwareHandler handler = dynamicHandlers.get(PROTOTYPE_HANDLER_KEY);
        if (handler == null) {
            handler = applicationContext.getBean(handlerClass);
            handler.setDynamicContent("_null_");
            dynamicHandlers.put(PROTOTYPE_HANDLER_KEY, handler);
        }

        return handler;
    }

    private DynamicContentAwareHandler resolveDynamicHandler(InternalEvent event) {
        LOGGER.debug("resolving dynamic handler for class {} and {}", handlerClass, event);
        Optional<String> oDynamicContent = InternalEvent.getDynamicContentAttribute(event);
        if (!oDynamicContent.isPresent()) {
            LOGGER.debug("No dynamic content found in (dynamic) {}", event);
            return null;
        }

        String dynamicContent = oDynamicContent.get();
        DynamicContentAwareHandler handler = dynamicHandlers.get(dynamicContent);
        if (handler == null) {
            handler = applicationContext.getBean(handlerClass);
            handler.setDynamicContent(dynamicContent);
            dynamicHandlers.put(dynamicContent, handler);
        }

        return handler;
    }

    private void cleanupDynamicContentHandler(InternalEvent event, DynamicContentAwareHandler handler) {
        Optional<String> oDynamicContent = InternalEvent.getDynamicContentAttribute(event);
        if (!oDynamicContent.isPresent()) {
            LOGGER.debug("No dynamic content found in (dynamic) event {}", event);
            return;
        }

        String dynamicContent = oDynamicContent.get();
        DynamicContentAwareHandler storedHandler = dynamicHandlers.get(dynamicContent);
        if (handler.equals(storedHandler)) {
            LOGGER.debug("Cleanup handler for dynamic content '{}'", dynamicContent);
            dynamicHandlers.remove(dynamicContent);
        }
    }
}
