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
        json = JSON.parse(message)
        raw_message = json['message']
        uuid = json.try(:[], 'properties').try(:[], 'x_request_id')
        @io.write '[%s] %s' % [uuid, raw_message]
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
