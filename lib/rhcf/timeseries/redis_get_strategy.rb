require_relative 'redis_string_based_strategy'

module Rhcf
  module Timeseries
    class RedisGetStrategy < RedisStringBasedStrategy
      def id
        'G'
      end

      def crunch_values(manager, evt_filter, resolution_id, time_point, subj_filter, limit = 100)
        events = events_for_subject_on(manager, evt_filter, time_point, resolution_id, subj_filter)

        values = {}
        events.each do |event|
          prefix = point_prefix(manager, evt_filter, resolution_id, time_point, event)

          value = get(manager, prefix)
          values[event] = value.to_i
        end
        values
      end

      def get(manager, a_key)
        manager.connection_to_use.get(a_key)
      end
    end
  end
end
