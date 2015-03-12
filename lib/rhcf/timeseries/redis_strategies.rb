module Rhcf
  module Timeseries
    class RedisHgetallStrategy
      def id
        'H'
      end

      def crunch_values(manager, subject, resolution_id, point, filter)
        values = hgetall(manager, EVENT_POINT_TOKEN, subject, resolution_id, point)
        values.reject!{|event, value| !filter.match(event) } if filter
        values
      end

      def store_point_value(manager, subject_path, resolution_name, resolution_value, point_value, event_path)
        key = [manager.prefix, EVENT_POINT_TOKEN ,subject_path, resolution_name, resolution_value].join(NAMESPACE_SEPARATOR)
        manager.connection_to_use.hincrby(key, event_path, point_value)
        manager.connection_to_use.expire(key, DEFAULT_RESOLUTIONS_MAP[resolution_name][:ttl])
      end

      def hgetall(manager, k,s,r,p)
        key  = [ manager.prefix, k,s,r,p].join(NAMESPACE_SEPARATOR)
        manager.connection_to_use.hgetall(key).each_with_object({}) do |(_k, value), hash|
          hash[_k] = value.to_i
        end
      end
    end

    class RedisStringBasedStrategy
      def id
        fail 'AbstractStrategy'
      end

      def store_point_value(manager, subject_path, resolution_name, resolution_value, point_value, event_path)
        store_point_event(manager, resolution_name, resolution_value, subject_path, event_path)
        key = [manager.prefix, EVENT_POINT_TOKEN ,subject_path, resolution_name, resolution_value, event_path].join(NAMESPACE_SEPARATOR)
        manager.connection_to_use.incrby(key, point_value)
        manager.connection_to_use.expire(key, DEFAULT_RESOLUTIONS_MAP[resolution_name][:ttl])
      end

      def store_point_event(manager, resolution_name, resolution_value, subject_path, event_path)
        key = [manager.prefix, EVENT_SET_TOKEN, resolution_name, resolution_value, subject_path].join(NAMESPACE_SEPARATOR)
        manager.connection_to_use.sadd(key, event_path)
        manager.connection_to_use.expire(key, DEFAULT_RESOLUTIONS_MAP[resolution_name][:ttl])
      end

      def events_for_subject_on(manager, subject, point, resolution_id)
        key = [manager.prefix, 'set', resolution_id, point, subject].join(NAMESPACE_SEPARATOR)
        manager.connection_to_use.smembers(key)
      end

    end

    class RedisMgetStrategy < RedisStringBasedStrategy
      def id
        'M'
      end

      def crunch_values(manager, subject, resolution_id, point, filter)
        events = events_for_subject_on(manager, subject, point, resolution_id)
        events = events.select{|event| filter.match(event) } if filter
        mget(manager, EVENT_POINT_TOKEN, subject, resolution_id, point, events)
      end

      def mget(manager, k, s, r, p, es)
        return {} if es.empty?
        keys = es.map{|e| [manager.prefix, k, s, r, p, e].flatten.join(NAMESPACE_SEPARATOR)}
        values = manager.connection_to_use.mget(*keys)
        data = {}
        keys.each_with_index do |key, index|
          data[es[index]] = values[index].to_i
        end
        data
      end
    end

    class RedisGetStrategy < RedisStringBasedStrategy
      def id
        'G'
      end

      def crunch_values(manager, subject, resolution_id, point, filter)
        events = events_for_subject_on(manager, subject, point, resolution_id)
        events = events.select{|event| filter.match(event) } if filter
        values = {}
        events.each do |event|
          value = get(manager, EVENT_POINT_TOKEN, subject, resolution_id, point, event)
          values[event] = value.to_i
        end
        values
      end

      def get(manager, *a_key)
        a_key = [manager.prefix, a_key].flatten.join(NAMESPACE_SEPARATOR)
        manager.connection_to_use.get(a_key)
      end
    end
  end
end
