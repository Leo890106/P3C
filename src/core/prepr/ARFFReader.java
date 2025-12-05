package core.prepr;

import java.io.*;
import java.util.*;
import java.util.zip.DataFormatException;

/**
 * ARFF reader for binary (0/1) features with support for SPARSE rows.
 * - Header: @relation / @attribute fX numeric (or anything; we treat as NOMINAL "0"/"1")
 * - Data:
 *   * Dense:  comma-separated 0/1 tokens, length = #attributes
 *   * Sparse: lines like "{i 1,  j 1,  ...}" or "{i:1, j:1}" (0-based indices)
 *
 * We only create the "1" selector for each attribute (frequency = count of 1's).
 * "0" is not instantiated as a selector (and will be ignored by convert_instance()).
 *
 * target_attr_count = 0  (pure association rule mining).
 */
public class ARFFReader extends DataReader {

    private BufferedReader input;          // for streaming next_record()
    private boolean hasHeader = true;

    // Parsed from header
    private final List<String> attrNames = new ArrayList<>();
    private boolean seenData = false;

    // Options
    public void setHasHeader(boolean b){ this.hasHeader = b; }

    public ARFFReader(){
        this.data_format = DATA_FORMATS.ARFF;
    }

    @Override
    public void bind_datasource(String data_filename) throws DataFormatException, IOException {
        if (this.data_format != DataReader.getDataFormat(data_filename))
            throw new DataFormatException("Require ARFF format");

        if (this.input != null) this.input.close();
        this.input = new BufferedReader(new FileReader(data_filename));

        // Position reader at the first line after "@data"
        String line;
        boolean inData = false;
        while ((line = this.input.readLine()) != null) {
            String s = line.trim();
            if (s.isEmpty() || s.startsWith("%")) continue;
            if (!inData) {
                if (s.equalsIgnoreCase("@data")) {
                    inData = true;
                    break;
                }
            } else {
                break; // shouldn't happen
            }
        }
    }

    @Override
    public void fetch_info(String data_filename, int target_attr_count, double support_threshold)
            throws DataFormatException, IOException {

        if (this.data_format != DataReader.getDataFormat(data_filename))
            throw new DataFormatException("Require ARFF format");

        // Reset state
        this.attributes.clear();
        this.attrNames.clear();
        this.seenData = false;

        int rows = 0;
        // 1) Parse header (@relation / @attribute) and remember attribute names
        try (BufferedReader br = new BufferedReader(new FileReader(data_filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                String s = line.trim();
                if (s.isEmpty() || s.startsWith("%")) continue;

                if (s.regionMatches(true, 0, "@relation", 0, 9)) {
                    continue; // ignore relation name
                } else if (s.regionMatches(true, 0, "@attribute", 0, 10)) {
                    // Extract attribute name (first token after @attribute)
                    // Accept: @attribute f0 numeric / @attribute 'f 0' numeric ...
                    String rest = s.substring(10).trim();
                    // Very tolerant name parsing: split by spaces, taking the first token as name (possibly quoted)
                    String name = parseAttributeName(rest);
                    attrNames.add(name);
                } else if (s.equalsIgnoreCase("@data")) {
                    this.seenData = true;
                    break;
                }
            }

            if (!this.seenData)
                throw new IOException("ARFF @data section not found");

            this.attr_count = attrNames.size();
            this.predict_attr_count = this.attr_count;
            this.target_attr_count = 0; // pure ARM

            // 2) First pass over data to count "1"s per attribute (works for both dense and sparse)
            int[] onesFreq = new int[this.attr_count];
            while ((line = br.readLine()) != null) {
                String s = line.trim();
                if (s.isEmpty() || s.startsWith("%")) continue;

                rows++;
                if (s.startsWith("{")) {
                    // Sparse row
                    Map<Integer,String> pairs = parseSparsePairs(s);
                    for (Map.Entry<Integer,String> e : pairs.entrySet()) {
                        int idx = e.getKey();
                        String val = e.getValue();
                        if (idx >= 0 && idx < this.attr_count && isOne(val)) {
                            onesFreq[idx]++;
                        }
                    }
                } else {
                    // Dense row (comma-separated tokens)
                    String[] toks = s.split(",", -1);
                    if (toks.length != this.attr_count) {
                        // tolerate short/long by clipping
                        int upto = Math.min(toks.length, this.attr_count);
                        for (int i = 0; i < upto; i++) if (isOne(toks[i])) onesFreq[i]++;
                    } else {
                        for (int i = 0; i < toks.length; i++) if (isOne(toks[i])) onesFreq[i]++;
                    }
                }
            }

            this.row_count = rows;
            // min sup absolute (floor)
            this.min_sup_count = (int)Math.floor(this.row_count * support_threshold);

            // 3) Build Attribute list: each attribute has only selector "1" with its frequency (if >0)
            for (int i = 0; i < this.attr_count; i++) {
                Attribute attr = new Attribute(i, attrNames.get(i), Attribute.DATA_TYPE.NOMINAL, new HashMap<String, Selector>());
                int f = onesFreq[i];
                if (f > 0) {
                    attr.distinct_values.put("1", new Selector(i, attr.name, "1", f));
                }
                this.attributes.add(attr);
            }

            // Prepare constructing selectors (frequent ones first etc.)
            this.prepare_selectors();
        }

        // 4) Ready for streaming
        bind_datasource(data_filename);
    }

    @Override
    public String[] next_record() throws IOException {
        if (this.input == null) return null;

        String line;
        while ((line = input.readLine()) != null) {
            String s = line.trim();
            if (s.isEmpty() || s.startsWith("%")) continue;

            if (s.startsWith("{")) {
                // Sparse -> expand to dense 0/1 tokens
                String[] dense = new String[this.attr_count];
                Arrays.fill(dense, "0");
                Map<Integer,String> pairs = parseSparsePairs(s);
                for (Map.Entry<Integer,String> e : pairs.entrySet()) {
                    int idx = e.getKey();
                    String val = e.getValue();
                    if (0 <= idx && idx < this.attr_count && isOne(val)) {
                        dense[idx] = "1";
                    }
                }
                return dense;
            } else {
                // Dense 0/1 tokens
                String[] toks = s.split(",", -1);
                if (toks.length == this.attr_count) return toks;

                // Tolerate length mismatch by padding/clipping
                String[] dense = new String[this.attr_count];
                Arrays.fill(dense, "0");
                int upto = Math.min(toks.length, this.attr_count);
                for (int i = 0; i < upto; i++) dense[i] = toks[i].trim();
                return dense;
            }
        }
        return null;
    }

    // ---------- helpers ----------

    private static boolean isOne(String v) {
        if (v == null) return false;
        v = v.trim();
        // Accept 1 / 1.0 / true / TRUE as "1"
        return v.equals("1") || v.equalsIgnoreCase("true") || v.equals("1.0");
    }

    private static String parseAttributeName(String rest) {
        rest = rest.trim();
        if (rest.isEmpty()) return "attr";
        // quoted?
        if (rest.charAt(0) == '\'' || rest.charAt(0) == '"') {
            char q = rest.charAt(0);
            int j = rest.indexOf(q, 1);
            if (j > 1) return rest.substring(1, j);
        }
        // else take first token
        int sp = rest.indexOf(' ');
        return (sp > 0) ? rest.substring(0, sp).trim() : rest;
    }

    private static Map<Integer,String> parseSparsePairs(String s) {
        // accept "{i 1,  j 1}" OR "{i:1, j:1}"
        Map<Integer,String> m = new HashMap<>();
        String inside = s;
        if (inside.startsWith("{") && inside.endsWith("}")) inside = inside.substring(1, inside.length()-1);
        if (inside.isEmpty()) return m;

        String[] segs = inside.split(",");
        for (String seg : segs) {
            String t = seg.trim();
            if (t.isEmpty()) continue;
            t = t.replace(':', ' ');               // normalize "i:1" -> "i 1"
            String[] iv = t.split("\\s+");
            if (iv.length >= 2) {
                try {
                    int idx = Integer.parseInt(iv[0]);
                    String val = iv[1].trim();
                    m.put(idx, val);
                } catch (NumberFormatException ignore) {}
            }
        }
        return m;
    }
}
