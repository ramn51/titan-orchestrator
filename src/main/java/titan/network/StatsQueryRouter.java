/*
 * Copyright 2026 Ram Narayanan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package titan.network;

import titan.scheduler.Scheduler;

import java.util.Locale;

/**
 * Typed query boundary for read-only scheduler observability.
 *
 * <p>All views share {@link TitanProtocol#OP_STATS_JSON}; the first payload field selects a
 * bounded projection. An empty payload remains the original full system snapshot contract.</p>
 */
final class StatsQueryRouter {
    private static final int MAX_TIMELINE = 5_000;
    private static final int MAX_HISTORY = 5_000;
    private static final int MAX_METRICS = 5_000;
    private static final int MAX_QUEUE = 500;

    private final Scheduler scheduler;

    StatsQueryRouter(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    String execute(String payload) {
        Query query = Query.parse(payload);
        return switch (query.view) {
            case SYSTEM -> scheduler.getSystemStatsJSON();
            case TIMELINE -> scheduler.timelineJSON(
                    query.nonNegativeLong(0, 0), query.limit(1, 1_000, MAX_TIMELINE));
            case STATUS -> scheduler.bulkStatusJSON(query.field(0));
            case QUEUE -> scheduler.queueBoardJSON(query.limit(0, 100, MAX_QUEUE));
            case HISTORY -> {
                String day = query.field(0);
                if (day.isBlank()) yield "ERROR: Missing UTC history date";
                yield scheduler.spanHistoryJSON(day, query.limit(1, 1_000, MAX_HISTORY));
            }
            case METRICS -> scheduler.metricsJSON(
                    query.nonNegativeLong(0, 0), query.limit(1, 200, MAX_METRICS));
            case UNKNOWN -> "ERROR: Unknown stats view: " + query.rawView;
        };
    }

    private enum View { SYSTEM, TIMELINE, STATUS, QUEUE, HISTORY, METRICS, UNKNOWN }

    private static final class Query {
        final View view;
        final String rawView;
        final String[] arguments;

        private Query(View view, String rawView, String[] arguments) {
            this.view = view;
            this.rawView = rawView;
            this.arguments = arguments;
        }

        static Query parse(String payload) {
            String value = payload == null ? "" : payload.trim();
            if (value.isEmpty() || value.equalsIgnoreCase("system")) {
                return new Query(View.SYSTEM, "system", new String[0]);
            }
            String[] fields = value.split("\\|", -1);
            String rawView = fields[0].trim().toLowerCase(Locale.ROOT);
            View view;
            try { view = View.valueOf(rawView.toUpperCase(Locale.ROOT)); }
            catch (IllegalArgumentException error) { view = View.UNKNOWN; }
            String[] arguments = new String[Math.max(0, fields.length - 1)];
            if (arguments.length > 0) System.arraycopy(fields, 1, arguments, 0, arguments.length);
            return new Query(view, rawView, arguments);
        }

        String field(int index) {
            return arguments.length > index ? arguments[index].trim() : "";
        }

        long nonNegativeLong(int index, long fallback) {
            try { return Math.max(0, Long.parseLong(field(index))); }
            catch (NumberFormatException error) { return fallback; }
        }

        int limit(int index, int fallback, int maximum) {
            try { return Math.max(1, Math.min(maximum, Integer.parseInt(field(index)))); }
            catch (NumberFormatException error) { return fallback; }
        }
    }
}
