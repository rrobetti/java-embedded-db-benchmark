package io.github.embeddedbench.harness;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Small JSON codec for report data; non-finite measurements are represented as null. */
public final class Json {
    private Json() {}

    public static String encode(Object value) {
        if (value == null) return "null";
        if (value instanceof String s) return quote(s);
        if (value instanceof Boolean) return value.toString();
        if (value instanceof Number n) {
            if ((n instanceof Double && !Double.isFinite(n.doubleValue()))
                    || (n instanceof Float && !Float.isFinite(n.floatValue()))) return "null";
            return n.toString();
        }
        if (value instanceof Map<?, ?> map) {
            var b = new StringBuilder("{");
            for (var entry : map.entrySet()) {
                if (b.length() > 1) b.append(',');
                b.append(quote(entry.getKey().toString())).append(':').append(encode(entry.getValue()));
            }
            return b.append('}').toString();
        }
        if (value instanceof Iterable<?> list) {
            var b = new StringBuilder("[");
            for (Object item : list) {
                if (b.length() > 1) b.append(',');
                b.append(encode(item));
            }
            return b.append(']').toString();
        }
        return quote(value.toString());
    }

    private static String quote(String s) {
        var b = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\b' -> b.append("\\b");
                case '\f' -> b.append("\\f");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 32 || Character.isSurrogate(c)) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
                }
            }
        }
        return b.append('"').toString();
    }

    public static Object decode(String input) {
        Parser parser = new Parser(input);
        Object result = parser.value();
        parser.space();
        if (parser.pos != input.length()) throw new IllegalArgumentException("Trailing JSON data");
        return result;
    }

    private static final class Parser {
        private final String text;
        private int pos;
        private Parser(String text) { this.text = text; }
        private void space() {
            while (pos < text.length() && Character.isWhitespace(text.charAt(pos))) pos++;
        }
        private char take() {
            if (pos == text.length()) throw new IllegalArgumentException("Unexpected end of JSON");
            return text.charAt(pos++);
        }
        private Object value() {
            space();
            if (pos == text.length()) throw new IllegalArgumentException("Empty JSON value");
            return switch (text.charAt(pos)) {
                case '"' -> string();
                case '{' -> object();
                case '[' -> array();
                case 't' -> literal("true", Boolean.TRUE);
                case 'f' -> literal("false", Boolean.FALSE);
                case 'n' -> literal("null", null);
                default -> number();
            };
        }
        private Object literal(String literal, Object value) {
            if (!text.startsWith(literal, pos)) throw new IllegalArgumentException("Invalid JSON literal");
            pos += literal.length();
            return value;
        }
        private String string() {
            take();
            var b = new StringBuilder();
            while (true) {
                char c = take();
                if (c == '"') return b.toString();
                if (c < 32) throw new IllegalArgumentException("Unescaped JSON control character");
                if (c != '\\') { b.append(c); continue; }
                switch (take()) {
                    case '"' -> b.append('"');
                    case '\\' -> b.append('\\');
                    case '/' -> b.append('/');
                    case 'b' -> b.append('\b');
                    case 'f' -> b.append('\f');
                    case 'n' -> b.append('\n');
                    case 'r' -> b.append('\r');
                    case 't' -> b.append('\t');
                    case 'u' -> {
                        if (pos + 4 > text.length()) throw new IllegalArgumentException("Short JSON escape");
                        b.append((char) Integer.parseInt(text.substring(pos, pos + 4), 16));
                        pos += 4;
                    }
                    default -> throw new IllegalArgumentException("Invalid JSON escape");
                }
            }
        }
        private Map<String, Object> object() {
            take();
            var result = new LinkedHashMap<String, Object>();
            space();
            if (pos < text.length() && text.charAt(pos) == '}') { pos++; return result; }
            while (true) {
                space();
                if (pos == text.length() || text.charAt(pos) != '"') throw new IllegalArgumentException("Expected JSON key");
                String key = string();
                space();
                if (take() != ':') throw new IllegalArgumentException("Expected JSON colon");
                result.put(key, value());
                space();
                char next = take();
                if (next == '}') return result;
                if (next != ',') throw new IllegalArgumentException("Expected JSON comma");
            }
        }
        private List<Object> array() {
            take();
            var result = new ArrayList<>();
            space();
            if (pos < text.length() && text.charAt(pos) == ']') { pos++; return result; }
            while (true) {
                result.add(value());
                space();
                char next = take();
                if (next == ']') return result;
                if (next != ',') throw new IllegalArgumentException("Expected JSON comma");
            }
        }
        private Number number() {
            int start = pos;
            while (pos < text.length() && "-+0123456789.eE".indexOf(text.charAt(pos)) >= 0) pos++;
            String value = text.substring(start, pos);
            if (!value.matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?"))
                throw new IllegalArgumentException("Invalid JSON number");
            if (value.indexOf('.') >= 0 || value.indexOf('e') >= 0 || value.indexOf('E') >= 0)
                return Double.valueOf(value);
            return Long.valueOf(value);
        }
    }
}
