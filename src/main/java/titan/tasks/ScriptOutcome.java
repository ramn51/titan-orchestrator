package titan.tasks;

/** Interprets the script handler's wire result before a worker reports completion. */
public final class ScriptOutcome {
    private ScriptOutcome() {}

    public static String callbackStatus(String result) {
        if (result == null || !result.startsWith("COMPLETED|")) return "FAILED";
        String[] parts = result.split("\\|", 3);
        if (parts.length != 3) return "FAILED";
        try {
            return Integer.parseInt(parts[1].trim()) == 0 ? "COMPLETED" : "FAILED";
        } catch (NumberFormatException ignored) {
            return "FAILED";
        }
    }
}
