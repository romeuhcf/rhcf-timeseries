module Rhcf
  module Timeseries
    class Query
      def initialize(subject, from, to, series, filter = nil, limit = 1000)
        from, to = to, from if from > to

        @series = series
        @subject = subject
        @from = from
        @to = to

        @filter = filter
        @limit = limit
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

          values = @series.crunch_values(@subject, resolution_id, point, @filter, @limit)

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
  end
end
