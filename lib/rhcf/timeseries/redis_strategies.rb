module Rhcf
  module Timeseries
    class RedisHgetallStrategy
      def id
        'H'
      end

      def crunch_values(manager, evt_filter, resolution_id, time_point, subj_filter)
        values = hgetall(manager, EVENT_POINT_TOKEN, evt_filter, resolution_id, time_point)
        values.reject!{|event, value| !subj_filter.match?(event) } if subj_filter
        values
      end

      def store_point_value(manager, event_path, resolution_id, resolution_val, subject_path, increment)
        key = point_prefix(manager, event_path, resolution_id, resolution_val)
        manager.connection_to_use.hincrby(key, subject_path, increment)
        manager.connection_to_use.expire(key, DEFAULT_RESOLUTIONS_MAP[resolution_id][:ttl])
      end

      def hgetall(manager, k,s,r,p)
        key  = [ manager.prefix, k,s,r,p].join(NAMESPACE_SEPARATOR)
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

    class RedisStringBasedStrategy

      def point_prefix(manager, evt_filter, resolution_id, time_point = nil, subj_path = nil)
        [manager.prefix, EVENT_POINT_TOKEN, evt_filter, resolution_id, time_point, subj_path].compact.join(NAMESPACE_SEPARATOR)
      end

      def set_prefix(manager, evt_filter, resolution_id, time_point = nil)
        [manager.prefix, EVENT_SET_TOKEN, evt_filter, resolution_id, time_point].compact.join(NAMESPACE_SEPARATOR)
      end


      def id
        fail 'AbstractStrategy'
      end

      def store_point_value(manager, event_path, resolution_name, resolution_val, subj_path, increment)
        set_key     = set_prefix(manager, event_path, resolution_name, resolution_val)
        counter_key = point_prefix(manager, event_path, resolution_name, resolution_val, subj_path)

        manager.connection_to_use.sadd(set_key, subj_path)
        manager.connection_to_use.incrby(counter_key, increment)

        manager.connection_to_use.expire(counter_key, DEFAULT_RESOLUTIONS_MAP[resolution_name][:ttl])
        manager.connection_to_use.expire(set_key, DEFAULT_RESOLUTIONS_MAP[resolution_name][:ttl])
      end

      def events_for_subject_on(manager, evt_filter, res_point, resolution_id, subj_filter)
        key = set_prefix(manager, evt_filter, resolution_id, res_point)
        events = manager.connection_to_use.smembers(key)
        events = events.select{|event| subj_filter.match?(event) } if subj_filter
        events
      end
    end

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

      def ranking(manager, evt_filter, resolution_id, points_on_range, subj_filter, limit)
        point_prefix = point_prefix(manager, evt_filter, resolution_id)
        set_prefix = set_prefix(manager, evt_filter, resolution_id)

        manager.connection_to_use.evalsha(evalsha_for(manager, :ranking),
                                          keys: points_on_range,
                                          argv: [
                                            evt_filter,
                                            subj_filter && subj_filter.to_lua_pattern,
                                            set_prefix,
                                            point_prefix,
                                            limit
                                          ])
      end

      def events_for_subject_on(manager, evt_filter, time_point, resolution_id, subj_filter)
        key = set_prefix(manager, resolution_id, evt_filter, time_point)
        events = if subj_filter
                   manager.connection_to_use.evalsha(evalsha_for(manager, :smembers_matching),
                                                     keys: [key], argv: [subj_filter.to_lua_pattern])
                 else
                   manager.connection_to_use.smembers(key)
                 end
        events
      end

      def crunch_values(manager, evt_filter, resolution_id, time_point, subj_filter)
        point_prefix = point_prefix(manager, resolution_id, evt_filter, time_point)
        set_key = point_prefix(manager, resolution_id, evt_filter, time_point)

        data = manager.connection_to_use.evalsha(evalsha_for(manager, :mget_matching_smembers),
                                                 keys: [set_key],
                                                 argv: [point_prefix, subj_filter && subj_filter.to_lua_pattern])

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

      def evalsha_for(manager, sym_os_lua_script)
        register_lua_scripts(manager.connection_to_use).fetch(sym_os_lua_script)
      end

      def register_lua_scripts(connection)

        @lua_script_register ||=
          begin
            ranking_script = <<-EOF
              local evt_filter   = ARGV[1]
              local subj_filter  = ARGV[2]
              local set_prefix   = ARGV[3]
              local point_prefix = ARGV[4]
              local limit        = tonumber(ARGV[5])

              local set_keys = {}
              for _, time in pairs(KEYS) do
                table.insert(set_keys, set_prefix .. '|' .. time)
              end

              local all_subjects = redis.call("SUNION", unpack(set_keys))
              local filtered_subjects = {}

              if subj_filter then
                for _, val in pairs(all_subjects) do
                  if string.match(val, subj_filter) then
                    table.insert(filtered_subjects, val)
                  end
                end
              else
                filtered_subjects = all_subjects
              end

              local counter_tuples = {}

              for _, subject in pairs(filtered_subjects) do
                local my_counter_keys = {}

                for _, time in pairs(KEYS) do
                  table.insert(my_counter_keys, point_prefix .. "|" .. time .. "|" .. subject)
                end

                local counter_total = 0
                for _, val in pairs(redis.call("MGET", unpack(my_counter_keys))) do
                  counter_total = counter_total + ( tonumber(val) or 0 )
                end

                table.insert(counter_tuples, {subject, counter_total} )
              end

              table.sort(counter_tuples, function(a, b) return b[2] < a[2] end )
              return {unpack(counter_tuples, 1, limit)}
            EOF

            smembers_matching = <<-EOF
              local matches = {}
              for _, val in pairs(redis.call('smembers', KEYS[1])) do
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
                redis.call('publish', 'log', "XDEBUG: " .. msg)
              end

              local function mget_in_batches(keys_to_mget)
                local step     = 1024
                local results  = {}
                local last_end = 0
                local partial  = {}


                local function mget_batch(ini , fin)
                  log("Getting from " .. ini .. ' to ' .. fin .. ' on a total of ' .. #keys_to_mget)

                  partial =  redis.call('MGET', unpack(keys_to_mget, ini, fin))
                  for _, value in pairs(partial) do table.insert(results, value) end
                end

                for ending = step, #keys_to_mget, step do
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
                for i, evt_filter in pairs(subjects) do
                  local value = values[i] or 0
                  dictionary[evt_filter] = (dictionary[evt_filter] or 0) + value
                end

                local tuples = {}
                for evt_filter, value in pairs(dictionary) do
                  table.insert(tuples, { evt_filter, value } )
                end

                table.sort(tuples, function(a, b) return b[2] < a[2] end )

                local new_subjects = {}
                local new_counts = {}

                for i, tuple in pairs(tuples) do
                  if #new_subjects >= limit  then break end

                  local evt_filter = tuple[1]
                  local value = tuple[2]

                  table.insert(new_subjects, evt_filter)
                  table.insert(new_counts, value)
                end

                return {new_subjects, new_counts}
              end

              log("SETKEY " .. set_key ) -- #.. " | KEY prefix: " .. key_prefix .. "  FILTER PATTERN: " .. filter_pattern)
--              log("SETKEY " .. set_key .. " | KEY prefix: " .. key_prefix .. "  FILTER PATTERN: " .. filter_pattern)

              for _, val in ipairs(redis.call('smembers', set_key)) do
                if (filter_pattern and string.match(val, filter_pattern)) or not filter_pattern then
                  table.insert(keys, val)
                  table.insert(keys_to_mget, key_prefix .. '#{NAMESPACE_SEPARATOR}' .. val)
                end
              end

              if table.getn(keys) > 0 then
                local values = mget_in_batches(keys_to_mget)
                -- local sorted = sort_and_limit_tuples(keys, values)
                -- log ("Values card " .. #values .. " | keys card: " .. #keys)
                -- return sorted
                return {keys, values}
              else
                return {{},{}}
              end
            EOF

            {
              mget_matching_smembers: connection.script(:load, mget_matching_smembers),
              smembers_matching: connection.script(:load, smembers_matching),
              ranking: connection.script(:load, ranking_script)
            }
          end
      end
    end

    class RedisGetStrategy < RedisStringBasedStrategy
      def id
        'G'
      end

      def crunch_values(manager, evt_filter, resolution_id, time_point, subj_filter, limit = 100)
        events = events_for_subject_on(manager, evt_filter, time_point, resolution_id, subj_filter)
        values = {}
        events.each do |event|
          value = get(manager, point_prefix(manager, evt_filter, resolution_id, time_point))
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
