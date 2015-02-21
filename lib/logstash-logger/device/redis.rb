require 'redis'
require 'stud/buffer'

module LogStashLogger
  module Device
    class Redis < Connectable
      include Stud::Buffer

      DEFAULT_LIST = 'logstash'

      attr_accessor :list

      def initialize(opts)
        super
        @list = opts.delete(:list) || DEFAULT_LIST
        @redis_options = opts
        @buffer = []
        @mutex = Mutex.new
        connect
        reset_buffer_clock!
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
        @mutex.synchronize do
          @buffer << message
          if buffer_timed_out?
            @io.commit
            reset_buffer_clock!
          end
        end
      end

      def flush
      end

      def close
        @io.commit
      rescue => e
        warn "#{self.class} - #{e.class} - #{e.message}"
      ensure
        @io = nil
      end

      def reset_buffer_clock!
        @last_commit_time = Time.now.to_f
      end

      def buffer_timed_out?
        (Time.now.to_f - @last_commit_time) >= 3.0
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
