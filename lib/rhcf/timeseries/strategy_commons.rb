require_relative 'prefix_commons'

module Rhcf
  module Timeseries
    module StrategyCommons
      include PrefixCommons
      def store_descending(manager, subj_path, descend_subject, evt_path, descend_event, resolution_name, resolution_value, point_value)
        descend(evt_path, descend_event) do |event_path|
          descend(subj_path, descend_subject) do |subject_path|
            store_point_value(manager, event_path, resolution_name, resolution_value, subject_path, point_value)
          end
        end
      end

      def descend(path, do_descend = true , &block)
        return if path.empty? || (path == ".")
        block.call(path)
        descend(File.dirname(path), do_descend, &block) if do_descend
      end
    end
  end
end
