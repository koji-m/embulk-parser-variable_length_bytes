Embulk::JavaPlugin.register_parser(
  "variable_length_bytes", "org.embulk.parser.variable_length_bytes.VariableLengthBytesParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
