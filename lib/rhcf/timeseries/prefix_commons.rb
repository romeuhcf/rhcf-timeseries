module Rhcf
  module Timeseries
    module PrefixCommons
      def point_prefix(manager, evt_filter, resolution_id, time_point = nil, subj_path = nil, event = nil)
        [manager.prefix, EVENT_POINT_TOKEN, evt_filter, resolution_id, time_point, subj_path, event].compact.join(NAMESPACE_SEPARATOR)
      end

      def set_prefix(manager, evt_filter, resolution_id, time_point = nil)
        [manager.prefix, EVENT_SET_TOKEN, evt_filter, resolution_id, time_point].compact.join(NAMESPACE_SEPARATOR)
      end
    end
  end
end
