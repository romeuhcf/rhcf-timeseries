module Rhcf
  module Timeseries
    EVENT_SET_TOKEN   = 'ES'
    EVENT_POINT_TOKEN = 'P'
    DEFAULT_PREFIX    = 'TS'

    DEFAULT_RESOLUTIONS_MAP = {
      ever: { span: Float::INFINITY, formatter: "ever", ttl: (2 * 366).days },
      year: { span: 365.days, formatter: "%Y", ttl: (2 * 366).days },
      week: { span: 1.week, formatter: "%Y-CW%w", ttl: 90.days },
      month: { span: 30.days, formatter: "%Y-%m", ttl: 366.days },
      day: { span: 1.day, formatter: "%Y-%m-%d", ttl: 30.days },
      hour: { span: 1.hour, formatter: "%Y-%m-%dT%H", ttl: 24.hours },
      minute: { span: 1.minute, formatter: "%Y-%m-%dT%H:%M", ttl: 120.minutes },
      second: { span: 1, formatter: "%Y-%m-%dT%H:%M:%S", ttl: 1.hour },
      "5seconds": { span: 5.seconds, formatter: ->(time) { [time.strftime("%Y-%m-%dT%H:%M:") ,  time.to_i % 60 / 5, '*', 5].join('') }, ttl: 1.hour },
      "5minutes": { span: 5.minutes, formatter: ->(time) { [time.strftime("%Y-%m-%dT%H:") ,  (time.to_i / 60) % 60 / 5, '*', 5].join('') }, ttl: 3.hour },
      "15minutes": { span: 15.minutes, formatter: ->(time) { [time.strftime("%Y-%m-%dT%H:") ,  (time.to_i / 60) % 60 / 15, '*', 15].join('') }, ttl: 24.hours }

    }

    DEFAULT_RESOLUTIONS = DEFAULT_RESOLUTIONS_MAP.keys
    NAMESPACE_SEPARATOR = '|'
  end
end
