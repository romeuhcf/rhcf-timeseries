module Rhcf
  module Timeseries
    class Query
      def initialize(evt_filter, from, to, series, subj_filter = nil)
        from, to = to, from if from > to

        @series = series
        @evt_filter = evt_filter
        @from = from
        @to = to

        @subj_filter = subj_filter
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

      def ranking(limit, resolution_id = nil)
        resolution_id ||= better_resolution[:id]
        points_on_range = point_range(resolution_id)
        @series.ranking(@evt_filter, resolution_id, points_on_range, @subj_filter, limit)
      end

      def points(resolution_id)
        list =[]

        point_range(resolution_id) do |point|

          values = @series.crunch_values(@evt_filter, resolution_id, point, @subj_filter)

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
        points_on_range = []
        resolution = @series.resolution(resolution_id)
        span = resolution[:span]
        ptr = @from.dup
        while ptr < @to
          point = @series.resolution_value_at(ptr, resolution_id)
          yield point if block_given?
          points_on_range << point
          ptr += span.to_i
        end

        points_on_range
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
  end
end
