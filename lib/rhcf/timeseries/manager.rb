unless 1.respond_to?(:minute)
  require_relative '../extensions/fixnum'
end
require 'rhcf/timeseries/constants'
require 'rhcf/timeseries/query'
require 'rhcf/timeseries/redis_strategies'

module Rhcf
  module Timeseries

    class Filter
      attr_reader :regex
      def initialize(keys, values)
        @keys   = keys
        @values = values
      end

      def regex
        @regex ||= Regexp.new('\A' + @keys.map{|key| @values[key]|| '.*'}.join('\/') + '\z')
      end

      def match?(value)
        value =~ regex
      end

      def to_lua_pattern
        @lua_pattern ||= @keys.map{|key| @values[key]|| '.*'}.join('/')
      end
    end

    class Manager
      DEFAULT_STRATEGY  = RedisHgetallStrategy
      attr_reader :prefix
      def initialize(options = {})
        @strategy          = ( options[:strategy]    || DEFAULT_STRATEGY).new
        @resolution_ids    =   options[:resolutions] || DEFAULT_RESOLUTIONS
        @prefix            = [(options[:prefix]      || DEFAULT_PREFIX) , @strategy.id].join(NAMESPACE_SEPARATOR)
        @connection_to_use =   options[:connection]
      end

      def on_connection(conn)
        old_connection = @connection_to_use
        @connection_to_use = conn
        yield self
        @connection_to_use = old_connection
      end

      def connection_to_use
        @connection_to_use || fail("No connection given")
      end

      def store(subject, event_point_hash, moment = Time.now, descend_subject = true, descend_event = true)
        resolutions = resolutions_of(moment)

        descend(subject, descend_subject) do |subject_path|
          event_point_hash.each do |event, point_value|
            descend(event, descend_event) do |event_path|
              resolutions.each do |res|
                resolution_name, resolution_value = *res
                store_point_value(subject_path, resolution_name, resolution_value, point_value, event_path)
              end
            end
          end
        end
      end

      def resolutions_of(moment)
        @resolution_ids.collect do |res_id|
          [res_id, resolution_value_at(moment, res_id)]
        end
      end

      def resolution_value_at(moment, res_id)
        res_config = DEFAULT_RESOLUTIONS_MAP[res_id] # TODO configurable
        if res_config.nil?
          fail "No resolution config for id: #{res_id.class}:#{res_id}"
        end

        time_resolution_formater = res_config[:formatter]
        case time_resolution_formater
        when String
          moment.strftime(time_resolution_formater)
        when Proc
          time_resolution_formater.call(moment)
        else
          fail ArgumentError, "Unexpected moment formater type #{time_resolution_formater.class}"
        end
      end

      def descend(path, do_descend = true , &block)
        return if path.empty? or path == "."
        block.call(path)
        descend(File.dirname(path), do_descend, &block) if do_descend
      end

      def store_point_value( subject_path, resolution_name, resolution_value, point_value, event_path)
        @strategy.store_point_value(self, subject_path, resolution_name, resolution_value, point_value, event_path)
      end

      def find(subject, from, to = Time.now, filter = nil)
        Rhcf::Timeseries::Query.new(subject, from, to, self, filter)
      end

      def resolution(id)
        res = DEFAULT_RESOLUTIONS_MAP[id] # TODO configurable
        fail ArgumentError, "Invalid resolution name #{id} for this time series" if res.nil?
        res.merge(:id => id)
      end

      def resolutions
        @_resolutions ||= @resolution_ids.map { |id| resolution(id) }
      end

      def crunch_values(subject, resolution_id, point, filter)
        @strategy.crunch_values(self, subject, resolution_id, point, filter)
      end
    end
  end
end
