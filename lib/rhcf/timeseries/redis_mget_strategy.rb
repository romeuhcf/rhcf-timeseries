module Rhcf
  module Timeseries
    class RedisMgetStrategy < RedisStringBasedStrategy
      def id
        'M'
      end

      def crunch_values(manager, evt_filter, resolution_id, time_point, subj_filter)
        events = events_for_subject_on(manager, evt_filter, time_point, resolution_id, subj_filter)
        mget(manager, EVENT_POINT_TOKEN, evt_filter, resolution_id, time_point, events)
      end

      def mget(manager, k, s, r, p, es)
        return {} if es.empty?
        keys = es.map { |e| [manager.prefix, k, s, r, p, e].flatten.join(NAMESPACE_SEPARATOR) }
        values = manager.connection_to_use.mget(*keys)
        data = {}
        keys.each_with_index do |key, index|
          data[es[index]] = values[index].to_i
        end
        data
      end
    end
  end
end
