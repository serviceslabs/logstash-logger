require 'redic'
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
        connect
        reset_buffer_clock!
      end

      def connect
        url = "redis://:#{@redis_options[:password]}@#{@redis_options[:host]}:6379/0"
        @io = ::Redic.new(url)
      end

      def reconnect
        connect
      end

      def with_connection
        connect unless @io
        yield
      rescue ::Exception
        reconnect
        retry
      rescue => e
        warn "#{self.class} - #{e.class} - #{e.message}"
        @io = nil
      end

      def write(message)
        @io.queue('RPUSH', @list, message)
        if buffer_timed_out?
          @io.commit
          reset_buffer_clock!
        end
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
        (Time.now.to_f - @last_commit_time) >= 2.0
      end

      def flush(*args)
        @io.commit if @io.buffer.length  >= 5
      end

    end
  end
end
