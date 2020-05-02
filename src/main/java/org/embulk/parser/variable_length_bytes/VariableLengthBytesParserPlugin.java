package org.embulk.parser.variable_length_bytes;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.util.FileInputInputStream;

import java.nio.charset.Charset;

public class VariableLengthBytesParserPlugin
        implements ParserPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("columns")
        public SchemaConfig getColumns();

        @Config("record_separator")
        @ConfigDefault("\"LF\"")
        public String getRecordSeparator();

        @Config("charset")
        @ConfigDefault("\"utf-8\"")
        public Charset getCharset();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        public boolean getStopOnInvalidRecord();
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
        boolean containsVarLenField = true;

        try (FileInputInputStream is = new FileInputInputStream(input);
             PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)
        ) {
            while (is.nextFile()) {
                final String fileName = input.hintOfCurrentInputFileNameForLogging().orElse("-");
                if (containsVarLenField) {
                }
                else {
                }
            }
        } catch (Exception e) {
            // ToDo
        }
    }
}
