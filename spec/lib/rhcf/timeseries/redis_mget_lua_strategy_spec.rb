require 'spec_helper'
require 'redis'
require 'rhcf/timeseries/manager'

describe Rhcf::Timeseries::RedisMgetLuaStrategy do
  it_behaves_like 'a valid strategy'
end
