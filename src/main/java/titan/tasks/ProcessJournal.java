package titan.tasks;

import titan.TitanConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** Process journal shared by workers; PID and process start time prevent PID-reuse kills. */
final class ProcessJournal {
    private static final Object JVM_LOCK = new Object();
    private static volatile Path testJournal;
    private record Entry(String owner, String id, long pid, long startedAt) { }

    private ProcessJournal() { }

    private static Path journal() {
        if (testJournal != null) return testJournal;
        String fallback = Paths.get(System.getProperty("java.io.tmpdir"), "titan-process-journal.tsv")
                .toAbsolutePath().toString();
        return Paths.get(TitanConfig.get("titan.process.journal", fallback)).toAbsolutePath().normalize();
    }

    static void setTestJournal(Path path) { testJournal = path; }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }

    private static String key(Entry entry) { return entry.owner + "\u0000" + entry.id; }

    private static Map<String, Entry> read(FileChannel channel) throws IOException {
        Map<String, Entry> entries = new LinkedHashMap<>();
        long size = channel.size();
        if (size > 16_000_000) throw new IOException("Process journal exceeds 16 MB");
        ByteBuffer data = ByteBuffer.allocate((int) size);
        channel.position(0);
        while (data.hasRemaining() && channel.read(data) > 0) { }
        for (String line : new String(data.array(), 0, data.position(), StandardCharsets.UTF_8).split("\\R")) {
            String[] parts = line.split("\t", -1);
            if (parts.length != 4) continue;
            try {
                Entry entry = new Entry(decode(parts[0]), decode(parts[1]),
                        Long.parseLong(parts[2]), Long.parseLong(parts[3]));
                entries.put(key(entry), entry);
            } catch (IllegalArgumentException ignored) {
                System.err.println("[PROCESS] Skipping malformed journal entry");
            }
        }
        return entries;
    }

    private static void write(FileChannel channel, Map<String, Entry> entries) throws IOException {
        StringBuilder text = new StringBuilder();
        for (Entry entry : entries.values()) {
            text.append(encode(entry.owner)).append('\t').append(encode(entry.id)).append('\t')
                    .append(entry.pid).append('\t').append(entry.startedAt).append('\n');
        }
        byte[] bytes = text.toString().getBytes(StandardCharsets.UTF_8);
        channel.truncate(0);
        channel.position(0);
        ByteBuffer data = ByteBuffer.wrap(bytes);
        while (data.hasRemaining()) channel.write(data);
        channel.force(true);
    }

    private static void update(Consumer<Map<String, Entry>> change) {
        synchronized (JVM_LOCK) {
            Path path = journal();
            try {
                Files.createDirectories(path.getParent());
                try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                        StandardOpenOption.READ, StandardOpenOption.WRITE);
                     FileLock ignored = channel.lock()) {
                    Map<String, Entry> entries = read(channel);
                    change.accept(entries);
                    write(channel, entries);
                }
            } catch (IOException error) {
                throw new IllegalStateException("Process journal update failed: " + error.getMessage(), error);
            }
        }
    }

    static void register(String owner, String id, long pid) {
        long startedAt = ProcessHandle.of(pid).flatMap(handle -> handle.info().startInstant())
                .map(Instant::toEpochMilli).orElse(0L);
        if (startedAt == 0) throw new IllegalStateException("Cannot record process start time for PID " + pid);
        update(entries -> { Entry entry = new Entry(owner, id, pid, startedAt); entries.put(key(entry), entry); });
    }

    static void unregister(String owner, String id) {
        update(entries -> entries.remove(owner + "\u0000" + id));
    }

    static void cleanup(String owner) {
        List<Entry> abandoned = new ArrayList<>();
        update(entries -> entries.values().removeIf(entry -> {
            if (!entry.owner.equals(owner)) return false;
            abandoned.add(entry);
            return true;
        }));
        for (Entry entry : abandoned) {
            ProcessHandle.of(entry.pid).ifPresent(process -> {
                long actualStart = process.info().startInstant().map(Instant::toEpochMilli).orElse(-1L);
                if (actualStart != entry.startedAt) return;
                System.err.println("[PROCESS] Cleaning abandoned tree " + entry.id + " PID " + entry.pid);
                process.descendants().forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly();
            });
        }
    }
}
