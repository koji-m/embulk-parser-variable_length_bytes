package org.embulk.parser.variable_length_bytes;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.util.FileInputInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;

import java.io.UnsupportedEncodingException;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class VariableLengthBytesParserPlugin
        implements ParserPlugin
{
    private static final ImmutableSet<String> TRUE_STRINGS =
            ImmutableSet.of(
                    "true", "True", "TRUE",
                    "yes", "Yes", "YES",
                    "t", "T", "y", "Y",
                    "on", "On", "ON",
                    "1");

    public interface PluginTask
            extends Task
    {
        @Config("columns")
        public SchemaConfig getColumns();

        @Config("record_separator")
        @ConfigDefault("\"LF\"")
        public Optional<String> getRecordSeparator();

        @Config("charset")
        @ConfigDefault("\"utf-8\"")
        public Charset getCharset();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        public boolean getStopOnInvalidRecord();
    }

    private List<Integer[]> getPositions(SchemaConfig schemaConfig)
    {
        boolean containsVarLen = false;
        List<Integer[]> positions = new ArrayList<>();
        for (ColumnConfig columnConfig : schemaConfig.getColumns()) {
            ConfigSource configSource = columnConfig.getOption();
            String[] pos = configSource.get(java.lang.String.class, "pos")
                    .split("\\.\\.");
            Integer pos1 = null;
            if (pos[1].equals(".")) {
                if (containsVarLen) {
                    throw new ConfigException("Variable length position must contains once.");
                }
                containsVarLen = true;
            }
            else {
                pos1 = Integer.parseInt(pos[1]);
            }
            positions.add(new Integer[]{Integer.parseInt(pos[0]), pos1});
        }
        return positions;
    }

    private List<Byte> parseRecordSeparator(Optional<String> separator)
    {
        List<Byte> res = new ArrayList<>();

        if (!separator.isPresent()) {
            return res;
        }

        String separatorStr = separator.get();
        switch (separatorStr) {
            case "CR":
                res.add(Byte.valueOf((byte) 0x0D));
                break;
            case "LF":
                res.add(Byte.valueOf((byte) 0x0A));
                break;
            case "CRLF":
                res.add(Byte.valueOf((byte) 0x0D));
                res.add(Byte.valueOf((byte) 0x0A));
                break;
            default:
                byte[] bytes = DatatypeConverter.parseHexBinary(separatorStr);
                for (byte b : bytes) {
                    res.add(Byte.valueOf(b));
                }
        }
        return res;
    }

    private boolean checkVarLenField(List<Integer[]> positions)
    {
        boolean res = false;
        for (Integer[] i : positions) {
            if (i[1] == null) {
                res = true;
                break;
            }
        }
        return res;
    }

    private int getRecordSize(List<Integer[]> positions)
    {
        java.util.Optional<Integer[]> maxOpt = positions.stream()
                .max((a, b) -> a[1] - b[1]);
        Integer[] max = maxOpt.get();
        return max[1];
    }

    private boolean invalidRecordSeparator(byte[] buf, List<Byte> recordSeparator,
                                           int recordSize, int recordSeparatorSize)
    {
        for (int i = 0; i < recordSeparatorSize; i++) {
            if (buf[recordSize + i] != recordSeparator.get(i)) {
                return true;
            }
        }
        return false;
    }

    private void extractRecord(byte[] buf, Schema schema, List<Integer[]> positions,
                               PageBuilder pageBuilder, String charset)
    {
        for (Column col : schema.getColumns()) {
            Integer[] pos = positions.get(col.getIndex());
            byte[] bytes = java.util.Arrays.copyOfRange(buf, pos[0], pos[1]);
            String data;
            try {
                data = new String(bytes, charset);
                switch (col.getType().getName()) {
                    case "boolean":
                        pageBuilder.setBoolean(col, TRUE_STRINGS.contains(data.trim()));
                        break;
                    case "long":
                        pageBuilder.setLong(col, Long.parseLong(data));
                        break;
                    case "double":
                        pageBuilder.setDouble(col, Double.parseDouble(data));
                        break;
                    case "string":
                        pageBuilder.setString(col, data);
                        break;
                }
            }
            catch (UnsupportedEncodingException e) {
                // ToDo
                System.err.println("UnsupportedEncoding");
            }
        }
        pageBuilder.addRecord();
    }

    private void extractVarLenRecord(byte[] buf, FileInputInputStream is, Schema schema, List<Integer[]> positions,
                                     PageBuilder pageBuilder, String charset, List<Byte> recordSeparator)
    {
        List<Column> columns = schema.getColumns();
        int numColumns = positions.size();
        for (int i = 0; i < numColumns - 1; i++) {
            Column col = columns.get(i);
            Integer[] pos = positions.get(i);
            byte[] bytes = java.util.Arrays.copyOfRange(buf, pos[0], pos[1]);
            String data;
            try {
                data = new String(bytes, charset);
                switch (col.getType().getName()) {
                    case "boolean":
                        pageBuilder.setBoolean(col, TRUE_STRINGS.contains(data.trim()));
                        break;
                    case "long":
                        pageBuilder.setLong(col, Long.parseLong(data));
                        break;
                    case "double":
                        pageBuilder.setDouble(col, Double.parseDouble(data));
                        break;
                    case "string":
                        pageBuilder.setString(col, data);
                        break;
                }
            }
            catch (UnsupportedEncodingException e) {
                // ToDo
                System.err.println("UnsupportedEncoding");
            }
        }
        Column varLenColumn = columns.get(numColumns - 1);
        Integer varLenPos = positions.get(numColumns - 1)[0];
        byte[] readByte = new byte[1];
        byte firstByteOfSeparator = recordSeparator.get(0);
        List<Byte> restByteOfSeparator = recordSeparator.subList(1, recordSeparator.size());
        List<Byte> varLenBytes = new ArrayList<>();
        List<Byte> sepBytes = new ArrayList<>();
        while (true) {
            int len = is.read(readByte, 0, 1);
            if (len < 0) {
                break;
            }
            byte b = readByte[0];
            if (b == firstByteOfSeparator) {
                boolean recordEnd = true;
                sepBytes.add(b);
                for (byte sep : restByteOfSeparator) {
                    len = is.read(readByte, 0, 1);
                    if (len < 0) {
                        recordEnd = false;
                        break;
                    }
                    b = readByte[0];
                    if (b != sep) {
                        recordEnd = false;
                        break;
                    }
                    sepBytes.add(b);
                }
                if (recordEnd) {
                    break;
                }
                varLenBytes.addAll(sepBytes);
                varLenBytes.add(b);
            }
            else {
                varLenBytes.add(b);
            }
        }
        try {
            String data;
            byte[] dataBytes = new byte[varLenBytes.size()];
            for (int i = 0; i < dataBytes.length; i++) {
                dataBytes[i] = varLenBytes.get(i);
            }
            data = new String(dataBytes, charset);
            switch (varLenColumn.getType().getName()) {
                case "boolean":
                    pageBuilder.setBoolean(varLenColumn, TRUE_STRINGS.contains(data.trim()));
                    break;
                case "long":
                    pageBuilder.setLong(varLenColumn, Long.parseLong(data));
                    break;
                case "double":
                    pageBuilder.setDouble(varLenColumn, Double.parseDouble(data));
                    break;
                case "string":
                    pageBuilder.setString(varLenColumn, data);
                    break;
            }
        }
        catch (UnsupportedEncodingException e) {
            // ToDo
            System.err.println("UnsupportedEncoding");
        }
        pageBuilder.addRecord();
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();

        control.run(task.dump(), schema);
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        List<Integer[]> positions = getPositions(task.getColumns());
        List<Byte> recordSeparator = parseRecordSeparator(task.getRecordSeparator());
        int recordSeparatorSize = recordSeparator.size();
        boolean containsVarLenField = checkVarLenField(positions);
        String charset = task.getCharset().name();
        boolean stopOnInvalidRecord = task.getStopOnInvalidRecord();

        if (recordSeparatorSize == 0 && containsVarLenField) {
            throw new ConfigException("If you have variable length field, you must specify a record separator.");
        }

        try (FileInputInputStream is = new FileInputInputStream(input);
             PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)
        ) {
            while (is.nextFile()) {
                final String fileName = input.hintOfCurrentInputFileNameForLogging().orElse("-");

                int totalLen = 0;
                if (containsVarLenField) {
                    int bufSize = getRecordSize(positions.subList(0, positions.size() - 1));
                    byte[] buf = new byte[bufSize];
                    while (true) {
                        int len = is.read(buf, totalLen, bufSize - totalLen);
                        if (len < 0) {
                            break;
                        }
                        totalLen += len;
                        if (totalLen < bufSize) {
                            continue;
                        }
                        extractVarLenRecord(buf, is, schema, positions, pageBuilder, charset, recordSeparator);
                        totalLen = 0;
                    }
                }
                else {
                    int recordSize = getRecordSize(positions);
                    int bufSize = recordSize + recordSeparatorSize;
                    byte[] buf = new byte[bufSize];
                    while (true) {
                        int len = is.read(buf, totalLen, bufSize - totalLen);
                        if (len < 0) {
                            break;
                        }
                        totalLen += len;
                        if (totalLen < bufSize) {
                            continue;
                        }
                        if (invalidRecordSeparator(buf, recordSeparator,
                                recordSize, recordSeparatorSize)) {
                            if (stopOnInvalidRecord) {
                                throw new DataException(String.format("Invalid record separator(%s): %d", fileName));
                            }
                            logger.warn(String.format("Skipped record: file: %s, record-separator:%d,%d:%s", fileName, buf[bufSize - 2], buf[bufSize - 1],
                                    new String(buf, java.nio.charset.Charset.forName("Shift_JIS"))));
                            totalLen = 0;
                            continue;
                        }
                        extractRecord(buf, schema, positions, pageBuilder, charset);
                        totalLen = 0;
                    }
                }
                pageBuilder.finish();
            }
        } catch (Exception e) {
            // ToDo
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(VariableLengthBytesParserPlugin.class);
}
