module Rhcf
  module Timeseries
    class RedisHgetallStrategy
      def id
        'H'
      end

      def crunch_values(manager, evt_filter, resolution_id, time_point, subj_filter)
        values = hgetall(manager, EVENT_POINT_TOKEN, evt_filter, resolution_id, time_point)
        values.reject! { |event, value| !subj_filter.match?(event) } if subj_filter
        values
      end

      def store_point_value(manager, event_path, resolution_id, resolution_val, subject_path, increment)
        key = point_prefix(manager, event_path, resolution_id, resolution_val)
        manager.connection_to_use.hincrby(key, subject_path, increment)
        manager.connection_to_use.expire(key, DEFAULT_RESOLUTIONS_MAP[resolution_id][:ttl])
      end

      def hgetall(manager, k, s, r, p)
        key = [ manager.prefix, k, s, r, p].join(NAMESPACE_SEPARATOR)
        manager.connection_to_use.hgetall(key).each_with_object({}) do |(_k, value), hash|
          hash[_k] = value.to_i
        end
      end

      def point_prefix(manager, evt_filter, resolution_id, time_point = nil, subj_path = nil)
        [manager.prefix, EVENT_POINT_TOKEN, evt_filter, resolution_id, time_point, subj_path].compact.join(NAMESPACE_SEPARATOR)
      end

      def set_prefix(manager, evt_filter, resolution_id, time_point = nil)
        [manager.prefix, EVENT_SET_TOKEN, evt_filter, resolution_id, time_point].compact.join(NAMESPACE_SEPARATOR)
      end
    end
  end
end
