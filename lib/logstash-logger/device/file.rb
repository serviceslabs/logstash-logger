require 'fileutils'

module LogStashLogger
  module Device
    class File < Base
      def initialize(opts)
        super
        @path = opts[:path] || fail(ArgumentError, "Path is required")
        open
      end

      def open
        unless ::File.exist? ::File.dirname @path
          ::FileUtils.mkdir_p ::File.dirname @path
        end

        @io = ::File.open @path, ::File::WRONLY | ::File::APPEND | ::File::CREAT
        @io.binmode
        @io.sync = self.sync
      end

      def write(message)
        raw_message = JSON.parse(message)['message']
        @io.write raw_message
        @io.write "\n"
      rescue JSON::ParserError
        @io.write message
        @io.write "\n"
      end

      def commit
        flush
      end

    end
  end
end
