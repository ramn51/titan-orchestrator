/*
 * Copyright 2026 Ram Narayanan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */

package titan.tasks;

/**
 * {@code TaskHandler} defines a contract for components that can execute a specific task.
 * Implementations of this interface are responsible for processing an input payload
 * and returning a result, typically as a String.
 * <p>
 * This interface also provides a default mechanism for setting a log listener,
 * allowing for external observation of task execution logs.
 */
    public interface TaskHandler {
    /**
 * Executes a task with the given payload.
 * Implementations should define the specific logic for processing the payload
 * and generating a result.
 * 
 * @param payload The input data or parameters for the task, typically in a serialized format like JSON or XML.
 * @return A string representing the result of the task execution. This could be a success message, a serialized object, or an error message.
 */
    public String execute(String payload);
    /**
 * Sets a listener to receive log messages generated during the task execution.
 * This default implementation does nothing, but concrete implementations can override it
 * to provide logging capabilities, for example, to send logs to a centralized logging system
 * or to a UI component.
 * 
 * @param listener A {@link java.util.function.Consumer} that accepts log messages as strings.
 *                 If {@code null}, any previously set listener should be removed or ignored.
 */
    default void setLogListener(java.util.function.Consumer<String> listener) {}
}
