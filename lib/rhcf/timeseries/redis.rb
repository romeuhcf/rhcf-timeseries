#!/usr/local/rvm/rubies/ruby-1.9.3-p194/bin/ruby
require 'active_support/core_ext/numeric/time'

#require 'observer'

require 'micon'
require 'date'

class Rhcf::Timeseries::Result
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
#require 'pry-debugger';binding.pry
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


class Rhcf::Timeseries::Redis
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

#require 'json'
#$:.unshift File.dirname(__FILE__)
#require 'hash_storer'

=begin
class Redis
#	include Observable
	attr_reader :prefix, :scales, :max_points
	
	
	def initialize(prefix, scales, max_points, store = HashStorer.new)
		@lasts = {}
		@prefix = prefix
		@scales = {}
		@store = store
		scales.each do |s|
			@scales[s] = eval(s)
		end
		@max_point = max_points
	end

	def persist
		@store.persist if @store.respond_to?( 'persist' )
	end


	def add(dimension, value, time = Time.now)
		@scales.each do |name, v|
			add_to_scale(name, dimension, value, time)
		end
	end	

	def add_to_scale(scale_name, dimension, value, time = Time.now)
		scale = @scales[scale_name]
		time = time.to_i
		scale_time = time - (time % scale)
		scale_key =  [@prefix, dimension, scale_name].join('-')
		last = @store.increment(scale_key, scale_time, value, time, @lasts["#{scale_name}-#{dimension}"])
		@lasts["#{scale_name}-#{dimension}"] = last['last']
		changed
		notify_observers(scale_key,  last)
	end
end


class RedisTimeSeriesUpdater
	def initialize(channel_prefix, observable, redis)
		@channel_prefix = channel_prefix
		@observable = observable
		@redis = redis
		observable.add_observer(self)
	end

	def update(key, data)
		key += '-realtime'
#		puts ['broadcasting', data, 'on', key].join(' ')
		@redis.publish(key, data.to_json)
	end
end

redis = Redis.new

ts1 = TimeSeries.new('mymachine',  ['1.hour', '15.minutes', '1.minute', '1.second', '10.seconds'], 1000)
#ts1 = TimeSeries.new('mymachine',  [ '1.second'], 1000)
pub = RedisTimeSeriesUpdater.new('realtime', ts1, redis)


alive = true
Signal.trap('INT'){
	alive = false
}

Signal.trap('USR1'){
	pub.refresh
}
while alive
	sleep 1
	ts1.add('cpu_load', %x{uptime | sed 's/.*: //;s/,.*//'}.strip.to_f, Time.now)
	ts1.add('rx', %x{ifconfig eth0| grep 'RX bytes' | sed 's/ (.*//;s/.*://'}.strip.to_f,Time.now)
	ts1.add('tx', %x{ifconfig eth0| grep 'TX bytes'  | sed 's/.*TX.*://;s/ .*//'}.strip.to_f,Time.now)
end

ts1.persist
=end
