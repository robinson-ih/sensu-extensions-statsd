require 'sensu/extension'

module Sensu
  module Extension
    class StatsDSimpleSocket < EM::Connection
      attr_accessor :data

      def receive_data(data)
        @data << data
      end
    end

    class StatsD < Check # rubocop:disable Metrics/ClassLength
      def name
        'statsd'
      end

      def description
        'a statsd implementation'
      end

      def options # rubocop:disable Metrics/MethodLength
        return @options if @options
        @options = {
          bind: '127.0.0.1',
          port: 8125,
          flush_interval: 10,
          send_interval: 30,
          percentile: 90,
          delete_gauges: false,
          delete_counters: false,
          delete_timers: false,
          reset_gauges: false,
          reset_counters: true,
          reset_timers: true,
          add_client_prefix: true,
          path_prefix: 'statsd',
          add_path_prefix: true,
          handler: 'graphite',
          truncate_output: true
        }
        @options.merge!(@settings[:statsd]) if @settings[:statsd].is_a?(Hash)
        @options
      end

      def definition
        check_attributes = {
          type: 'metric',
          name: name,
          interval: options[:send_interval],
          standalone: true,
          output_format: 'graphite_plaintext',
          handler: options[:handler],
          truncate_output: options[:truncate_output]
        }
        options[:additional_attributes] ? check_attributes.merge!(options[:additional_attributes]) : check_attributes
      end

      def post_init
        @flush_timers = []
        @data = EM::Queue.new
        @gauges_list = []
        @counters_list = []
        @timers_list = []
        @metrics = []
        setup_flush_timers
        setup_parser
        setup_statsd_socket
      end

      def run
        output = ''
        if @metrics
          output << @metrics.join("\n") + "\n" unless @metrics.empty?
          @logger.info('statsd collected metrics', count: @metrics.count)
          @metrics = []
        end
        yield output, 0
      end

      private
      def add_metric(*args) # rubocop:disable Metrics/MethodLength
        tags = args.pop
        value = args.pop
        path = []
        path << @settings[:client][:name] if options[:add_client_prefix]
        path << options[:path_prefix] if options[:add_path_prefix]
        path = (path + args).join('.')
        if path !~ /^[A-Za-z0-9\._-]*$/
          @logger.info('invalid statsd metric', reason: 'metric path must only consist of alpha-numeric characters, periods, underscores, and dashes',
                                                path: path,
                                                value: value)
        else
          @logger.debug('adding statsd metric', path: path,
                                                value: value)
          graphite_metric = [path, value, Time.now.to_i]
          graphite_metric << tags.collect {|k,v| k.to_s + ':' + v.to_s}.join(',') unless tags.nil? || tags.empty?
          @metrics << graphite_metric.join(' ')
        end
      end

      def flush!
        @gauges_list.each do |metric|
          add_metric('gauges', metric[:name], metric[:value], metric[:tags])
        end
        @gauges_list = []
        @counters_list.each do |metric|
          add_metric('counters', metric[:name], metric[:value].to_i, metric[:tags])
        end
        @counters_list = []
        @timers_list.each do |metric|
          values = metric[:values]
          next if values.empty?
          values.sort!
          length = values.length
          min = values.first || 0
          max = values.last || 0
          mean = min
          max_at_threshold = min
          percentile = options[:percentile]
          if length > 1
            threshold_index = ((100 - percentile) / 100.0) * length
            threshold_count = length - threshold_index.round
            valid_values = values.slice(0, threshold_count)
            max_at_threshold = valid_values[-1]
            sum = 0
            valid_values.each { |v| sum += v }
            mean = sum / valid_values.length
          end
          add_metric('timers', metric[:name], 'lower', min, metric[:tags])
          add_metric('timers', metric[:name], 'mean', mean, metric[:tags])
          add_metric('timers', metric[:name], 'upper', max, metric[:tags])
          add_metric('timers', metric[:name], "upper_#{percentile}", max_at_threshold, metric[:tags])
        end
        @timers_list = []
        @logger.debug('flushed statsd metrics')
      end

      def setup_flush_timers
        @flush_timers << EM::PeriodicTimer.new(options[:flush_interval]) do
          flush!
        end
      end

      def create_or_fetch_matching_tags(list, name, tags, data_format)
        data_matched = list.find { |data| data[:tags] == tags && data[:name] == name }
        if data_matched.nil?
          data_matched = data_format
          data_matched[:tags] = tags
          list << data_matched
        end
        data_matched
      end

      # TODO: come back and refactor me
      def setup_parser # rubocop:disable Metrics/MethodLength
        parser = proc do |data|
          begin
            nv, type, *extras = data.strip.split('|')
            name, raw_value = nv.split(':')
            value = Float(raw_value)
            sample = Float(1)
            statsd_tags = {}
            extras.each do |param|
              case param
              when /^@/
                sample = Float(param.split('@').last)
              when /^#/
                statsd_tags = param.split('#').last.split(',')
                statsd_tags = statsd_tags.reduce({}) { |all_tags, datagram_tag|
                                k,v = datagram_tag.split(':')
                                all_tags[k] = v
                                all_tags
                              }
              end
            end
            case type
            when 'g'
              new_gauge = { name: name, value: 0, tags: {} }
              matched_gauge = create_or_fetch_matching_tags(@gauges_list, name, statsd_tags, new_gauge)
              matched_gauge[:value] = value
            when /^c/, 'm'
              new_counter = { name: name, value: 0, tags: {} }
              value = value * (1 / sample)
              matched_counter = create_or_fetch_matching_tags(@counters_list, name, statsd_tags, new_counter)
              matched_counter[:value] += value
            when 'ms', 'h', 't'
              new_timer = { name: name, values: [], tags: {} }
              value = value * (1 / sample)
              matched_timer = create_or_fetch_matching_tags(@timers_list, name, statsd_tags, new_timer)
              matched_timer[:values] << value
            end
          rescue => error
            @logger.error('statsd parser error', error: error.to_s)
          end
          EM.next_tick do
            @data.pop(&parser)
          end
        end
        @data.pop(&parser)
      end

      def setup_statsd_socket
        @logger.debug('binding statsd tcp and udp sockets', options: options)
        bind = options[:bind]
        port = options[:port]
        EM.start_server(bind, port, StatsDSimpleSocket) do |socket|
          socket.data = @data
        end
        EM.open_datagram_socket(bind, port, StatsDSimpleSocket) do |socket|
          socket.data = @data
        end
      end
    end
  end
end
