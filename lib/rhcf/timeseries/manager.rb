unless 1.respond_to?(:minute)
  require_relative '../extensions/fixnum'
end
require 'rhcf/timeseries'
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
        @regex ||= Regexp.new('\A' + @keys.map { |key| @values[key] || '.*' }.join('\/') + '\z')
      end

      def match?(value)
        value =~ regex
      end

      def to_lua_pattern
        @lua_pattern ||= @keys.map { |key| @values[key] || '.*' }.join('/')
      end
    end

    class Manager
      DEFAULT_STRATEGY = RedisHgetallStrategy
      attr_reader :prefix
      def initialize(options = {})
        @strategy          = (options[:strategy] || DEFAULT_STRATEGY).new
        @resolution_ids    =   options[:resolutions] || DEFAULT_RESOLUTIONS
        @prefix            = [(options[:prefix]      || DEFAULT_PREFIX) , @strategy.id].join(NAMESPACE_SEPARATOR)
        @connection_to_use =   options[:connection]
      end

      def find(evt_filter, from, to = Time.now.utc, subj_filter = nil)
        Rhcf::Timeseries::Query.new(evt_filter, from, to, self, subj_filter)
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

      def store(evt_path, subject_point_hash, moment = Time.now.utc, descend_subject = true, descend_event = true)
        resolutions = resolutions_of(moment)

        descend(evt_path, descend_event) do |event_path|
          subject_point_hash.each do |subj_path, point_value|
            descend(subj_path, descend_subject) do |subject_path|

              resolutions.each do |res|
                resolution_name, resolution_value = *res
                store_point_value(event_path, resolution_name, resolution_value, subject_path, point_value)
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
        moment = moment.utc
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
        return if path.empty? || (path == ".")
        block.call(path)
        descend(File.dirname(path), do_descend, &block) if do_descend
      end

      def store_point_value(subject_path, resolution_name, resolution_value, point_value, event_path)
        @strategy.store_point_value(self, subject_path, resolution_name, resolution_value, point_value, event_path)
      end

      def resolution(id)
        res = DEFAULT_RESOLUTIONS_MAP[id] # TODO configurable
        fail ArgumentError, "Invalid resolution name #{id} for this time series" if res.nil?
        res.merge(id: id)
      end

      def resolutions
        @_resolutions ||= @resolution_ids.map { |id| resolution(id) }
      end

      def ranking(evt_filter, resolution_id, points_on_range, subj_filter, limit)
        @strategy.ranking(self, evt_filter, resolution_id, points_on_range, subj_filter, limit)
      end

      def crunch_values(evt_filter, resolution_id, point, subj_filter)
        @strategy.crunch_values(self, evt_filter, resolution_id, point, subj_filter)
      end
    end
  end
end
