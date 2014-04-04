require 'active_support/core_ext/numeric/time'
require 'micon'
require 'date'

module Rhcf
  module Timeseries

    class Result
      def initialize(subject, from, to, series)
        if from > to
          raise ArgumentError, "Argument 'from' can not be bigger then 'to'"
        end
        @series = series
        @subject = subject
        @from = from
        @to = to
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
          values = {}

          @series.events_for_subject_on(@subject, point, resolution_id).each do |event|
            value = @series.get('point', @subject, event, resolution_id, point)
            values[event] = value.to_i
          end

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
        span = @to - @from
        resolutions = @series.resolutions.sort_by{|h| h[:span]}.reverse
        better = resolutions.find{|r| r[:span] < span / 5} || resolutions.last
      end
    end


    class Redis
      inject :logger
      inject :redis_connection

      RESOLUTIONS_MAP={
          :ever => {span:Float::INFINITY, formatter: "ever"},
          :year => {span: 365.days,formatter: "%Y"},
          :week => {span: 1.week, formatter: "%Y-CW%w"},
          :month => {span: 30.days, formatter: "%Y-%m"},
          :day => {span: 1.day, formatter: "%Y-%m-%d"},
          :hour => {span: 1.hour, formatter: "%Y-%m-%dT%H"},
          :minute => {span: 1.minute, formatter: "%Y-%m-%dT%H:%M"},
          :second => {span: 1, formatter: "%Y-%m-%dT%H:%M:%S"},
          :"5seconds" => {span: 5.seconds, formatter: ->(time){ [time.strftime("%Y-%m-%dT%H:%M:") ,  time.to_i % 60/5, '*',5].join('') }},
          :"5minutes" => {span: 5.minutes, formatter: ->(time){ [time.strftime("%Y-%m-%dT%H:") ,  (time.to_i/60) % 60/5, '*',5].join('') }},
          :"15minutes" => {span: 15.minutes, formatter: ->(time){ [time.strftime("%Y-%m-%dT%H:") ,  (time.to_i/60) % 60/15, '*',15].join('') }}

      }
      DEFAULT_RESOLUTIONS = RESOLUTIONS_MAP.keys
      DEFAULT_MAX_POINTS = 1_024
      NAMESPACE_SEPARATOR = '|'


      def initialize(options = {})
        @resolution_ids = options[:resolutions] || DEFAULT_RESOLUTIONS
        @max_points = options[:max_points] || DEFAULT_MAX_POINTS
        @prefix=self.class.name
      end

      def store(subject, event_point_hash, moment = Time.now)
        descend(subject) do |subject_path|
          event_point_hash.each do |event, point_value|
            descend(event) do |event_path|
              resolutions_of(moment) do |resolution_name, resolution_value|
                store_point_value(subject_path, event_path, resolution_name, resolution_value, point_value)
              end
            end
          end
        end
      end


      def resolutions_of(moment)
        @resolution_ids.each do |res_id|
          yield res_id, resolution_value_at(moment, res_id)
        end
      end

      def resolution_value_at(moment, res_id)
        time_resolution_formater = RESOLUTIONS_MAP[res_id][:formatter]
        case time_resolution_formater
          when String
            moment.strftime(time_resolution_formater)
          when Proc
            time_resolution_formater.call(moment)
          else
            raise ArgumentError, "Unexpected moment formater type #{time_resolution_formater.class}"
        end
      end


      def descend(path, &block)
        return if path.empty? or path == "."
        block.call(path)
        descend(File.dirname(path), &block)
      end

      def store_point_event( resolution_name, resolution_value, subject_path, event_path)
        key = [@prefix, 'event_set', resolution_name, resolution_value, subject_path].join(NAMESPACE_SEPARATOR)
        logger.debug("EVENTSET SADD #{key} -> #{event_path}")
        redis_connection.sadd(key, event_path)
      end

      def store_point_value( subject_path, event_path, resolution_name, resolution_value, point_value)
        store_point_event(resolution_name, resolution_value, subject_path, event_path)

        key = [@prefix, 'point' ,subject_path, event_path, resolution_name, resolution_value].join(NAMESPACE_SEPARATOR)
        logger.debug("SETTING KEY #{key}")
        redis_connection.incrby(key, point_value)
      end

      def find(subject, from, to = Time.now)
        Rhcf::Timeseries::Result.new(subject, from, to, self)
      end

      def flush!
        every_key{|a_key| delete_key(a_key)}
      end

      def every_key(pattern=nil, &block)
        pattern = [@prefix, pattern,'*'].compact.join(NAMESPACE_SEPARATOR)
        redis_connection.keys(pattern).each do |key|
          yield key
        end
      end

      def delete_key(a_key)
        logger.debug("DELETING KEY #{a_key}")
        redis_connection.del(a_key)
      end

      def keys(*a_key)
        raise "GIVEUP"
        a_key = [@prefix, a_key].flatten.join(NAMESPACE_SEPARATOR)
        logger.debug("FINDING KEY #{a_key}")
        redis_connection.keys(a_key).collect{|k| k.split(NAMESPACE_SEPARATOR)[1,1000].join(NAMESPACE_SEPARATOR) }
      end

      def get(*a_key)
        a_key = [@prefix, a_key].flatten.join(NAMESPACE_SEPARATOR)
        logger.debug("GETTING KEY #{a_key}")
        redis_connection.get(a_key)
      end


      def resolution(id)
        res = RESOLUTIONS_MAP[id]
        raise ArgumentError, "Invalid resolution name #{id} for this time series" if res.nil?
        res.merge(:id => id)
      end
      def resolutions
        @resolution_ids.collect do |id|
          resolution(id)
        end
      end


      def events_for_subject_on(subject, point, resolution_id)
        key = [@prefix, 'event_set', resolution_id, point, subject].join(NAMESPACE_SEPARATOR)
        logger.debug("EVENTSET SMEMBERS #{key}")
        redis_connection.smembers(key)
      end
    end
  end
end
