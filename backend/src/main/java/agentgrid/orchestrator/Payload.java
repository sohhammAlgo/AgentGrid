package agentgrid.orchestrator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Key/value encoding of a Subtask payload or Result output: one "key=value" per line,
 * with backslash and newline escaped in values. Values may themselves be encoded payloads
 * (SYNTHESIZE carries the SUMMARIZE outputs this way).
 */
public final class Payload {

    private final LinkedHashMap<String, String> values = new LinkedHashMap<>();

    public Payload put(String key, Object value) {
        values.put(key, String.valueOf(value));
        return this;
    }

    public String get(String key) {
        return values.get(key);
    }

    public String get(String key, String fallback) {
        return values.getOrDefault(key, fallback);
    }

    public int getInt(String key, int fallback) {
        String v = values.get(key);
        if (v == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public Map<String, String> asMap() {
        return Collections.unmodifiableMap(values);
    }

    /** Values of keys prefix0, prefix1, ... in order. */
    public List<String> list(String prefix) {
        List<String> out = new ArrayList<>();
        for (int i = 0; values.containsKey(prefix + i); i++) {
            out.add(values.get(prefix + i));
        }
        return out;
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : values.entrySet()) {
            if (sb.length() > 0) {
                sb.append('\n');
            }
            sb.append(e.getKey()).append('=').append(escape(e.getValue()));
        }
        return sb.toString();
    }

    /**
     * Parses an encoded payload. Text that is not in key=value form (for example the free
     * text sent by the Exp 1 / Exp 2 panels) is returned as a payload whose "query" is the text.
     */
    public static Payload decode(String text) {
        Payload p = new Payload();
        if (text == null || text.isEmpty()) {
            return p;
        }
        for (String line : text.split("\n")) {
            int eq = line.indexOf('=');
            if (eq <= 0 || !line.substring(0, eq).matches("[A-Za-z][A-Za-z0-9_]*")) {
                return new Payload().put("query", text);
            }
            p.values.put(line.substring(0, eq), unescape(line.substring(eq + 1)));
        }
        return p;
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "");
    }

    private static String unescape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(++i);
                sb.append(n == 'n' ? '\n' : n);
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
