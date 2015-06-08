module Rhcf
  module Timeseries
    class RedisHgetallStrategy
      def id
        'H'
      end

      def crunch_values(manager, subject, resolution_id, point, filter, limit = 100)
        values = hgetall(manager, EVENT_POINT_TOKEN, subject, resolution_id, point)
        values.reject!{|event, value| !filter.match?(event) } if filter
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

      def events_for_subject_on(manager, subject, point, resolution_id, filter)
        key = [manager.prefix, EVENT_SET_TOKEN, resolution_id, point, subject].join(NAMESPACE_SEPARATOR)
        events = manager.connection_to_use.smembers(key)
        events = events.select{|event| filter.match?(event) } if filter
        events
      end
    end

    class RedisMgetStrategy < RedisStringBasedStrategy
      def id
        'M'
      end

      def crunch_values(manager, subject, resolution_id, point, filter, limit = 100)
        events = events_for_subject_on(manager, subject, point, resolution_id, filter)
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

    class RedisMgetLuaStrategy < RedisMgetStrategy
      def id; 'ME'; end

      def events_for_subject_on(manager, subject, point, resolution_id, filter)
        key = [manager.prefix, EVENT_SET_TOKEN, resolution_id, point, subject].join(NAMESPACE_SEPARATOR)
        events = if filter
                   manager.connection_to_use.evalsha(evalsha_for(:smembers_matching),
                                                     keys: [key], argv: [filter.to_lua_pattern])
                 else
                   manager.connection_to_use.smembers(key)
                 end
        events
      end

      def crunch_values(manager, subject, resolution_id, point, filter, limit = 1000)
        register_lua_scripts!(manager.connection_to_use)
        point_prefix = [manager.prefix, EVENT_POINT_TOKEN, subject, resolution_id, point].join(NAMESPACE_SEPARATOR)
        set_key = [manager.prefix, EVENT_SET_TOKEN, resolution_id, point, subject].join(NAMESPACE_SEPARATOR)

        data = manager.connection_to_use.evalsha(evalsha_for(:mget_matching_smembers),
                                                 keys: [set_key], argv: [point_prefix, filter && filter.to_lua_pattern, limit])

        return {} if data.nil?
        result = {}
        begin
        data.first.each_with_index do |evt, idx|
          value = data.last[idx].to_i
          result[evt] = value
        end
        rescue
          p $!, $!.message
          raise
        end

        result
      end

      def evalsha_for(sym_os_lua_script)
        @lua_script_register[sym_os_lua_script] || fail("Script for '#{sym_os_lua_script}' not registered")
      end

      def register_lua_scripts!(connection)

        @lua_script_register ||=
          begin
            smembers_matching = <<-EOF
              local matches = {}
              for _, val in ipairs(redis.call('smembers', KEYS[1])) do
                if string.match(val, ARGV[1]) then
                  table.insert(matches, val)
                end
              end
              return matches
            EOF

            mget_matching_smembers = <<-EOF
              local set_key = KEYS[1]
              local key_prefix = ARGV[1]
              local filter_pattern = ARGV[2]
              local limit = tonumber(ARGV[3])
              local keys = {}
              local keys_to_mget = {}

              local function log(msg)
                -- redis.call('publish', 'log', msg)
              end

              local function mget_in_batches(keys_to_mget)
                local step = 1024
                local results = {}
                local last_end = 0
                local partial = {}

                local function mget_batch(ini , fin)
                  log("Getting from " .. ini .. ' to ' .. fin .. ' on a total of ' .. #keys_to_mget)
                  partial =  redis.call('MGET', unpack(keys_to_mget, ini, fin))
                  for _, value in pairs(partial) do table.insert(results, value) end
                end

                for ending = step,  #keys_to_mget, step do
                  mget_batch(last_end + 1, ending)
                  last_end = ending
                end

                if last_end < #keys_to_mget then
                  mget_batch(last_end + 1, #keys_to_mget)
                end

                return results;
              end

              local function sort_and_limit_tuples(subjects, values)
                local dictionary = {}
                for i, subject in pairs(subjects) do
                  local value = values[i] or 0
                  -- redis.call('publish', 'log', subject .. ' += ' .. value)
                  dictionary[subject] = (dictionary[subject] or 0) + value
                end

                local tuples = {}
                for subject, value in pairs(dictionary) do
                  -- redis.call('publish', 'log', subject .. ' = ' .. value)
                  table.insert(tuples, { subject, value } )
                end

                table.sort(tuples, function(a, b) return b[2] < a[2] end )

                local new_subjects = {}
                local new_counts = {}

                for i, tuple in pairs(tuples) do
                  if #new_subjects >= limit  then break end

                  local subject = tuple[1]
                  local value = tuple[2]

                  table.insert(new_subjects, subject)
                  table.insert(new_counts, value)
                end

                return {new_subjects, new_counts}
              end

              for _, val in ipairs(redis.call('smembers', set_key)) do
                if (filter_pattern and string.match(val, filter_pattern)) or not filter_pattern then
                  table.insert(keys, val)
                  table.insert(keys_to_mget, key_prefix .. '#{NAMESPACE_SEPARATOR}' .. val)
                end
              end

              if table.getn(keys) > 0 then
                local values = mget_in_batches(keys_to_mget)
                local sorted = sort_and_limit_tuples(keys, values)
                log ("Values card " .. #values .. " | keys card: " .. #keys)
                return sorted
              else
                return {{},{}}
              end
            EOF

            {
              mget_matching_smembers: connection.script(:load, mget_matching_smembers),
              smembers_matching: connection.script(:load, smembers_matching)
            }
          end
      end
    end

    class RedisGetStrategy < RedisStringBasedStrategy
      def id
        'G'
      end

      def crunch_values(manager, subject, resolution_id, point, filter, limit = 100)
        events = events_for_subject_on(manager, subject, point, resolution_id, filter)
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
