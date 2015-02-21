require 'redis'
require 'stud/buffer'

module LogStashLogger
  module Device
    class Redis < Connectable
      include Stud::Buffer

      DEFAULT_LIST = 'logstash'
      BUFFER_SIZE = 5

      attr_accessor :list

      def initialize(opts)
        super
        @list = opts.delete(:list) || DEFAULT_LIST
        @redis_options = opts
        @buffer = []
        @mutex = Mutex.new
        connect
      end

      def connect
        return if @io
        @io = ::Redis.new(@redis_options)
      end

      def reconnect
        @io.client.reconnect
      end

      def with_connection
        connect unless @io
        yield
      rescue ::Redis::InheritedError
        reconnect
      rescue => e
        warn "#{self.class} - #{e.class} - #{e.message}"
        @io = nil
      end

      def write(message)
        @buffer << message
        commit if buffer_overflow?
      end

      def flush
      end

      def close
        @io.close
      rescue => e
        warn "#{self.class} - #{e.class} - #{e.message}"
      ensure
        @io = nil
      end

      def buffer_overflow?
        @buffer.size > BUFFER_SIZE
      end

      def commit
        @mutex.synchronize do
          @io.pipelined do
            @buffer.each do |msg|
              @io.rpush @list, msg
            end
          end
          @buffer.clear
        end
      end

    end
  end
end
