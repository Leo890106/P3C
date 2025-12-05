/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 *
 */

/*
 * @author Van Quoc Phuong Huynh, FAW JKU
 * patched for robust CSV parsing by ChatGPT
 */
package core.prepr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

/**
 * Treat all attributes as NOMINAL (no discretization here).
 * Robust to quoted CSV (delimiter in quotes), trims values, and
 * aligns row width to the declared attribute count (pad/truncate).
 */
public class CSVReader extends DataReader {

    public CSVReader(){
        this.data_format = DATA_FORMATS.CSV;
    }

    /** Quote-aware split for CSV. Supports delimiter inside double quotes and escaped "" */
    private List<String> smartSplit(String line, char delim) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                // escaped quote
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == delim && !inQuotes) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        out.add(cur.toString());
        return out;
    }

    /** Normalize a raw token: trim; empty/NA -> '?' (null symbol) */
    private String normalizeToken(String v) {
        if (v == null) return "?";
        v = v.trim();
        if (v.isEmpty() || v.equalsIgnoreCase("NA")) return "?";
        // strip surrounding quotes if present
        if (v.length() >= 2 && v.charAt(0) == '"' && v.charAt(v.length()-1) == '"') {
            v = v.substring(1, v.length()-1).replace("\"\"", "\"").trim();
            if (v.isEmpty()) return "?";
        }
        return v;
    }

    /** read first non-empty line (header), tolerant of BOM/blank lines */
    private String readFirstDataLine(BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.length() > 0) {
                // strip BOM if any
                if (line.charAt(0) == '\uFEFF') line = line.substring(1);
                if (!line.trim().isEmpty()) return line;
            }
        }
        return null;
    }

    @Override
    public void bind_datasource(String data_filename) throws DataFormatException, IOException {
        if (this.data_format != DataReader.getDataFormat(data_filename))
            throw new DataFormatException("Require CSV format");

        if (this.input != null) this.input.close();

        this.input = new BufferedReader(new FileReader(data_filename));
        // skip header row (one line) robustly
        readFirstDataLine(this.input); // discard header line
    }

    @Override
    public void fetch_info(String data_filename,
                           int target_attr_count,
                           double support_threshold) throws DataFormatException, IOException {
  
        if (this.data_format != DataReader.getDataFormat(data_filename))
            throw new DataFormatException("Require CSV format");

        BufferedReader br = new BufferedReader(new FileReader(data_filename));
        String header = readFirstDataLine(br);
        if (header == null) {
            br.close();
            throw new IOException("Empty CSV: " + data_filename);
        }

        // 1) parse header → attributes (all NOMINAL)
        List<String> headerCols = smartSplit(header, this.delimiter.charAt(0));
        this.attributes.clear();
        int attr_id = -1;
        for (String name : headerCols) {
            attr_id++;
            String attr_name = name == null ? ("col" + attr_id) : name.trim();
            if (attr_name.isEmpty()) attr_name = "col" + attr_id;
            Attribute attr = new Attribute(attr_id, attr_name, Attribute.DATA_TYPE.NOMINAL,
                                           new HashMap<String, Selector>());
            this.attributes.add(attr);
        }

        this.attr_count = this.attributes.size();
        this.target_attr_count = target_attr_count;
        this.predict_attr_count = Math.max(0, this.attr_count - this.target_attr_count);

        // 2) scan data rows → build distinct_values (ATOM selectors)
        String line;
        this.row_count = 0;
        int dataLines = 0;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;
            dataLines++;
            if (dataLines <= 3) System.err.println("[CSV sample] " + line);
            List<String> toks = smartSplit(line, this.delimiter.charAt(0));

            // align to attr_count
            if (toks.size() < this.attr_count) {
                int need = this.attr_count - toks.size();
                for (int i = 0; i < need; i++) toks.add("?");
            } else if (toks.size() > this.attr_count) {
                toks = toks.subList(0, this.attr_count);
            }

            // count the row and update per-attribute distincts
            this.row_count++;

            for (int i = 0; i < this.attr_count; i++) {
                String value = normalizeToken(toks.get(i));
                if (Attribute.NULL_SYMBOLS.contains(value)) continue;

                Attribute attr = this.attributes.get(i);
                Selector s = attr.distinct_values.get(value);
                if (s == null) {
                    attr.distinct_values.put(value, new Selector(i, attr.name, value, 1));
                } else {
                    s.frequency++;
                }
            }
        }
        System.err.println("[CSV info] headerCols=" + this.attr_count + " dataLines=" + dataLines);
        br.close();

        // 3) min sup count = ceil(row_count * threshold) (at least 1 if threshold>0)
        if (support_threshold > 0) {
            this.min_sup_count = Math.max( (int)Math.ceil(this.row_count * support_threshold), 1 );
        } else {
            this.min_sup_count = 0;
        }

        // 4) prepare selector structures
        this.prepare_selectors();
        // or: this.prepare_selectors_PredictTargetSelectors_in_one_group();
    }

    /** Override to ensure data reading uses the same robust CSV parsing as fetch_info */
    @Override
    public String[] next_record() throws IOException {
        if (this.input == null) return null;
        String line;
        while ((line = this.input.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;
      
            List<String> toks = smartSplit(line, this.delimiter.charAt(0));

            // 對齊欄位數：不足補 "?"，多的截斷
            if (toks.size() < this.attr_count) {
                int need = this.attr_count - toks.size();
                for (int i = 0; i < need; i++) toks.add("?");
            } else if (toks.size() > this.attr_count) {
                toks = toks.subList(0, this.attr_count);
            }
            String[] arr = new String[this.attr_count];   // ← 絕不回 0 長
            for (int i = 0; i < this.attr_count; i++) {
                arr[i] = normalizeToken(toks.get(i));
            }
            return arr;
        }
        return null; // EOF
    }

}
