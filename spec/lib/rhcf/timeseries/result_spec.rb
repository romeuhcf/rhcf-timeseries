require 'spec_helper'
require 'rhcf/timeseries/redis'


describe "Query" do
  describe "#better_resolution" do

let(:redis_connection){nil}
    describe "When having a smaller then 1/5 " do
      let(:series) { Rhcf::Timeseries::Redis.new(redis_connection, resolutions: [:hour, :"15minutes", :minute]) }
      it { expect(series.find('bla', Time.now - 3600, Time.now).better_resolution[:id]).to eq :minute }
    end

    describe "When having a smaller but greather then 1/5" do
      let(:series) { Rhcf::Timeseries::Redis.new(redis_connection,  resolutions: [:hour, :"15minutes"]) }
      it { expect(series.find('bla', Time.now - 3600, Time.now).better_resolution[:id]).to eq :"15minutes" }
    end

    describe "When having no smaller, only its size" do
      let(:series) { Rhcf::Timeseries::Redis.new(redis_connection,  resolutions: [:hour]) }
      it { expect(series.find('bla', DateTime.parse('2015-01-01 01:50:00'), DateTime.parse('2015-01-01 03:10:00')).better_resolution[:id]).to eq :hour }
    end

    describe "When having only bigger" do
      let(:series) { Rhcf::Timeseries::Redis.new(redis_connection,  resolutions: [:month]) }
      it { expect(series.find('bla', DateTime.parse('2015-01-01 01:50:00'), DateTime.parse('2015-01-01 03:10:00')).better_resolution).to be_nil}
    end

  end
end
