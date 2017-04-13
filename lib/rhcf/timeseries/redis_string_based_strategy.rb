module Rhcf
  module Timeseries
    class RedisStringBasedStrategy
      def point_prefix(manager, evt_filter, resolution_id, time_point = nil, subj_path = nil, event = nil)
        [manager.prefix, EVENT_POINT_TOKEN, evt_filter, resolution_id, time_point, subj_path, event].compact.join(NAMESPACE_SEPARATOR)
      end

      def set_prefix(manager, evt_filter, resolution_id, time_point = nil)
        [manager.prefix, EVENT_SET_TOKEN, evt_filter, resolution_id, time_point].compact.join(NAMESPACE_SEPARATOR)
      end

      def id
        fail 'AbstractStrategy'
      end

      def store_point_value(manager, event_path, resolution_name, resolution_val, subj_path, increment, expire = false)
        set_key     = set_prefix(manager, event_path, resolution_name, resolution_val)
        counter_key = point_prefix(manager, event_path, resolution_name, resolution_val, subj_path)
        manager.connection_to_use.sadd(set_key, subj_path)
        manager.connection_to_use.incrby(counter_key, increment)

        if expire
          manager.connection_to_use.expire(counter_key, DEFAULT_RESOLUTIONS_MAP[resolution_name][:ttl])
          manager.connection_to_use.expire(set_key, DEFAULT_RESOLUTIONS_MAP[resolution_name][:ttl])
        end
      end

      def events_for_subject_on(manager, evt_filter, res_point, resolution_id, subj_filter)
        key = set_prefix(manager, evt_filter, resolution_id, res_point)
        events = manager.connection_to_use.smembers(key)
        events = events.select { |event| subj_filter.match?(event) } if subj_filter
        events
      end
    end
  end
end
