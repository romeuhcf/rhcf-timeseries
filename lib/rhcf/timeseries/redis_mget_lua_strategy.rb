module Rhcf
  module Timeseries
    class RedisMgetLuaStrategy < RedisStringBasedStrategy
      def id; 'ME'; end


      def ranking(manager, evt_filter, resolution_id, points_on_range, subj_filter, limit)
        point_prefix = point_prefix(manager, evt_filter, resolution_id)
        set_prefix = set_prefix(manager, evt_filter, resolution_id)

        manager.connection_to_use.evalsha(evalsha_for(manager, :ranking), keys: points_on_range, argv: [ evt_filter, subj_filter && subj_filter.to_lua_pattern, set_prefix, point_prefix, limit ])
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

      def crunch_values(manager, evt_filter, resolution_id, time_point, subj_filter = nil)
        point_prefix = point_prefix(manager,  evt_filter, resolution_id, time_point)
        set_key = set_prefix(manager, evt_filter, resolution_id, time_point)

        data = manager.connection_to_use.evalsha(evalsha_for(manager, :mget_matching_smembers),
                                                 keys: [set_key],
                                                 argv: [point_prefix, subj_filter&.to_lua_pattern])

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
          redis.call('publish', 'log','asmembers('.. KEYS[1]..')')
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



        local function mget_in_batches(keys_to_mget)
        local step     = 1024
        local results  = {}
        local last_end = 0
        local partial  = {}


        local function mget_batch(ini , fin)

        partial =  redis.call('MGET', unpack(keys_to_mget, ini, fin))
        for _, value in pairs(partial) do
          table.insert(results, value) end
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
redis.call('publish', 'log','bsmembers('.. set_key..')')
if filter_pattern then
  redis.call('publish', 'log', '>>>> FLT:' .. filter_pattern)
end
      for _, val in ipairs(redis.call('smembers', set_key)) do

        redis.call('publish', 'log', '>>>> VAL:' .. val)


        if (filter_pattern and string.match(val, filter_pattern)) or not filter_pattern then
          redis.call('publish', 'log', '>>>>> MATCH')
          table.insert(keys, val)
          table.insert(keys_to_mget, key_prefix .. '#{NAMESPACE_SEPARATOR}' .. val)
        end
      end

      if table.getn(keys) > 0 then
        local values = mget_in_batches(keys_to_mget)
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
  end
end
