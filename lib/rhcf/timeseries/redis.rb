unless 1.respond_to?(:minute)
  require_relative '../extensions/fixnum'
end

module Rhcf
  module Timeseries
    STRATEGY          = :hgetall
    EVENT_SET_TOKEN   = 'ES'
    EVENT_POINT_TOKEN = 'P'

    class Result
      def initialize(subject, from, to, series, filter = nil)
        if from > to
          fail ArgumentError, "Argument 'from' can not be bigger then 'to'"
        end
        @series = series
        @subject = subject
        @from = from
        @to = to

        @filter = filter
      end

      def total(resolution_id=nil)
        accumulator={}
        points(resolution_id || better_resolution[:id]) do |data|

          data[:values].each do |key, value|
            accumulator[key]||=0
            accumulator[key]+=value
          end
        end
        accumulator
      end

      def points(resolution_id)
        list =[]

        point_range(resolution_id) do |point|

          values = @series.crunch_values(@subject, resolution_id, point, @filter)

          next if values.empty?
          data =  {moment: point, values: values }
          if block_given?
            yield data
          else
            list << data
          end
        end
        list unless block_given?
      end

      def point_range(resolution_id)
        resolution = @series.resolution(resolution_id)
        span = resolution[:span]
        ptr = @from.dup
        while ptr < @to
          point = @series.resolution_value_at(ptr, resolution_id)
          yield point
          ptr += span.to_i
        end
      rescue FloatDomainError
        # OK
      end

      def better_resolution
        span = @to.to_time - @from.to_time

        resolutions = @series.resolutions.sort_by{|h| h[:span]}.reverse
        5.downto(1) do |div|
          res = resolutions.find{|r| r[:span] < span / div }
          return res if res
        end
        return nil
      end
    end


    class Redis

      RESOLUTIONS_MAP={
        :ever => {span:Float::INFINITY, formatter: "ever", ttl: (2 * 366).days},
        :year => {span: 365.days,formatter: "%Y", ttl: (2 * 366).days},
        :week => {span: 1.week, formatter: "%Y-CW%w", ttl: 90.days},
        :month => {span: 30.days, formatter: "%Y-%m", ttl: 366.days},
        :day => {span: 1.day, formatter: "%Y-%m-%d", ttl: 30.days},
        :hour => {span: 1.hour, formatter: "%Y-%m-%dT%H", ttl: 24.hours},
        :minute => {span: 1.minute, formatter: "%Y-%m-%dT%H:%M", ttl: 120.minutes},
        :second => {span: 1, formatter: "%Y-%m-%dT%H:%M:%S", ttl: 1.hour},
        :"5seconds" => {span: 5.seconds, formatter: ->(time){ [time.strftime("%Y-%m-%dT%H:%M:") ,  time.to_i % 60/5, '*',5].join('') }, ttl: 1.hour},
        :"5minutes" => {span: 5.minutes, formatter: ->(time){ [time.strftime("%Y-%m-%dT%H:") ,  (time.to_i/60) % 60/5, '*',5].join('') }, ttl: 3.hour},
        :"15minutes" => {span: 15.minutes, formatter: ->(time){ [time.strftime("%Y-%m-%dT%H:") ,  (time.to_i/60) % 60/15, '*',15].join('') }, ttl: 24.hours}

      }
      DEFAULT_RESOLUTIONS = RESOLUTIONS_MAP.keys
      NAMESPACE_SEPARATOR = '|'

      def initialize(redis,  options = {})
        @resolution_ids = options[:resolutions] || DEFAULT_RESOLUTIONS
        @prefix = [(options[:prefix] || self.class.name) , STRATEGY.to_s[0,1]].join(NAMESPACE_SEPARATOR)
        @connection_to_use = redis
      end

      def on_connection(conn)
        old_connection = @connection_to_use
        @connection_to_use = conn
        yield self
        @connection_to_use = old_connection
      end

      def redis_connection_to_use
        @connection_to_use || fail("No redis connection given")
      end

      def store(subject, event_point_hash, moment = Time.now, descend_subject = true, descend_event = true)
        resolutions = resolutions_of(moment)

        descend(subject, descend_subject) do |subject_path|
          event_point_hash.each do |event, point_value|
            descend(event, descend_event) do |event_path|
              resolutions.each do |res|
                resolution_name, resolution_value = *res
                store_point_value(subject_path, event_path, resolution_name, resolution_value, point_value)
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
        res_config = RESOLUTIONS_MAP[res_id]
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

      def store_point_event( resolution_name, resolution_value, subject_path, event_path)
        key = [@prefix, EVENT_SET_TOKEN, resolution_name, resolution_value, subject_path].join(NAMESPACE_SEPARATOR)
        redis_connection_to_use.sadd(key, event_path)
        redis_connection_to_use.expire(key, RESOLUTIONS_MAP[resolution_name][:ttl])
      end

      def store_point_value( subject_path, event_path, resolution_name, resolution_value, point_value)

        key = [@prefix, EVENT_POINT_TOKEN ,subject_path, resolution_name, resolution_value].join(NAMESPACE_SEPARATOR)
        if STRATEGY == :hgetall
          redis_connection_to_use.hincrby(key, event_path, point_value)
        else
          store_point_event(resolution_name, resolution_value, subject_path, event_path)
          key = [@prefix, EVENT_POINT_TOKEN ,subject_path, resolution_name, resolution_value, event_path].join(NAMESPACE_SEPARATOR)
          redis_connection_to_use.incrby(key, point_value)
        end
        redis_connection_to_use.expire(key, RESOLUTIONS_MAP[resolution_name][:ttl])
      end

      def find(subject, from, to = Time.now, filter = nil)
        Rhcf::Timeseries::Result.new(subject, from, to, self, filter)
      end

      def flush!
        every_key{|a_key| delete_key(a_key)}
      end

      def every_key(pattern=nil, &block)
        pattern = [@prefix, pattern,'*'].compact.join(NAMESPACE_SEPARATOR)
        redis_connection_to_use.keys(pattern).each do |key|
          yield key
        end
      end

      def delete_key(a_key)
        redis_connection_to_use.del(a_key)
      end

      def keys(*a_key)
        fail "GIVEUP"
        a_key = [@prefix, a_key].flatten.join(NAMESPACE_SEPARATOR)
        redis_connection_to_use.keys(a_key).collect{|k| k.split(NAMESPACE_SEPARATOR)[1,1000].join(NAMESPACE_SEPARATOR) }
      end

      def get(*a_key)
        a_key = [@prefix, a_key].flatten.join(NAMESPACE_SEPARATOR)
        redis_connection_to_use.get(a_key)
      end

      def hgetall(k,s,r,p)
        key  = [ @prefix, k,s,r,p].join(Redis::NAMESPACE_SEPARATOR)
        redis_connection_to_use.hgetall(key).each_with_object({}) do |(k, value), hash|
          hash[k] = value.to_i
        end
      end

      def mget(k, s, r, p, es)
        return {} if es.empty?
        keys = es.map{|e| [@prefix, k, s, r, p, e].flatten.join(NAMESPACE_SEPARATOR)}
        values = redis_connection_to_use.mget(*keys)
        data = {}
        keys.each_with_index do |key, index|
          data[es[index]] = values[index].to_i
        end
        data
      end

      def resolution(id)
        res = RESOLUTIONS_MAP[id]
        fail ArgumentError, "Invalid resolution name #{id} for this time series" if res.nil?
        res.merge(:id => id)
      end

      def resolutions
        @_resolutions ||= @resolution_ids.map { |id| resolution(id) }
      end

      def events_for_subject_on(subject, point, resolution_id)
        key = [@prefix, 'set', resolution_id, point, subject].join(NAMESPACE_SEPARATOR)
        redis_connection_to_use.smembers(key)
      end

      def crunch_values(subject, resolution_id, point, filter)
        case STRATEGY
        when :hgetall
          values = hgetall(EVENT_POINT_TOKEN, subject, resolution_id, point)
          values.reject!{|event, value| !filter.match(event) } if filter
          values
        when :mget
          events = events_for_subject_on(subject, point, resolution_id)
          events = events.select{|event| filter.match(event) } if filter
          values = mget(EVENT_POINT_TOKEN, subject, resolution_id, point, events)
        when :get
          events = events_for_subject_on(subject, point, resolution_id)
          events = events.select{|event| filter.match(event) } if filter
          values = {}
          events.each do |event|
            value = get(EVENT_POINT_TOKEN, subject, resolution_id, point, event)
            values[event] = value.to_i
          end
          values
        end
      end

    end
  end
end
