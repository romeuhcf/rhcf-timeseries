require 'spec_helper'
require 'rhcf/timeseries/manager'

require 'timecop'
require 'redis'
require 'benchmark'
require 'stackprof'
require 'securerandom'

def generate_subjects(n_pages, n_edits)
  edits = [].tap do  |me|
    1.upto(n_edits) do |i|
      me << "edit#{i}"
    end
  end
  [].tap { |me| n_pages.times { me << [edits.sample, SecureRandom.hex].join('/') } }
end

def generate_pageviews(evts_count, subjects, mintime, maxtime)
  pts = []
  evts_count.times do
    time = Time.at(rand((mintime.to_i)..(maxtime.to_i)))
    pts << [ subjects.sample , time]
  end
  pts
end

xdescribe Rhcf::Timeseries::Manager do
  let(:redis) { Redis.new }
  subject { Rhcf::Timeseries::Manager.new(connection: redis) }

  before(:each) do
    Timecop.return
  end

  describe 'descending' do
    it "is fast to store and read" do
      total = 0
      start_time = Time.now

      bench = Benchmark.measure {
        StackProf.run(mode: :cpu, out: p('/tmp/stackprof-cpu-store-descend.dump')) do
          1000.times do
            total += 1
            subject.store("a/b", "e/f" => 1) #, time)
          end
        end
      }

      Benchmark.measure {
        expect(subject.find("a", start_time - 11100, Time.now + 11100).total['e'].to_i).to eq(total)
      }

      Benchmark.measure {
        expect(subject.find("a", start_time - 100000, Time.now + 100000).total(:year)['e/f'].to_i).to eq(total)
      }

      puts "Descend write speed %d points/seg | points:%d, duration:%0.3fs" % [speed = (1.0 * total / (bench.total + 0.00000001)), total, bench.total]
      expect(speed).to be > 100
    end
  end

  describe 'not descending' do
    it "is be fast to store and read" do
      total = 0
      bench = Benchmark.measure {
        StackProf.run(mode: :cpu, out: p('/tmp/stackprof-cpu-store-nodescend.dump')) do
          1000.times do
            total += 1
            subject.store("a/b/c/d", { "e/f/g/h" => 1 }  , Time.now, false, false)
          end
        end
      }

      puts "No descend write speed %d points/seg | points:%d, duration:%0.3fs" % [new_speed = (1.0 * total / (bench.total + 0.00000001)), total, bench.total]
      expect(new_speed).to be > 300
    end
  end

  describe 'ranking' do
    subject do
      Rhcf::Timeseries::Manager.new(connection: redis,
                                    resolutions: [:hour],
                                    strategy: Rhcf::Timeseries::RedisMgetLuaStrategy)
    end

    let(:n_evts)  { 3000 }
    let(:n_pages) { 15 }
    let(:n_edits) { 3 }
    let(:start_time) { Time.parse('2015-01-01') }
    let(:end_time) { Time.parse('2015-01-02') }

    let(:subjects) { generate_subjects(n_pages, n_edits) }
    let(:points)  { generate_pageviews(n_evts, subjects, start_time, end_time) }

    before do
      points.each do |subject_and_time|
        evt_subject, time = *subject_and_time
        subject.store('pageview', { evt_subject => 1 }, time, false, false)
      end
    end

    let(:query) { subject.find("pageview", start_time, end_time) }

    let(:top10_forca_bruta) {
      acc = {}
      points.each do |item|
        s, _t = * item
        acc[s] ||= 0
        acc[s] += 1

      end
      acc.sort_by { |i| i.last }.reverse[0, 10]
    }

    it do
      expect(points.count).to eq n_evts
      expect(query.ranking(10).count).to eq 10
      expect(query.ranking(10)).to eq top10_forca_bruta
      expect(query.points(:hour).count).to eq 24
      expect(redis.keys('*').count).to eq 24 + 24 * n_pages # ao infinito
    end

  end

  describe "find and total" do
    let(:start_time) {  Time.parse("2000-01-01 00:00:00") }
    before do
      Timecop.travel(start_time)
      subject.store("views/product/15", "web/firefox/3" => 1)

      Timecop.travel(15.minutes) #00:00:15
      subject.store("views/product/13", { "web/firefox/3" => 1 }, Time.now)
      subject.store("views/product/13", { "web/firefox/3" => 1 }, Time.now)
      subject.store("views/product/13", { "web/firefox/3" => 0 }, Time.now)

      Timecop.travel(15.minutes) #00:00:30
      subject.store("views/product/15", "web/ie/6" => 3)

      Timecop.travel(15.minutes) #00:00:45
      subject.store("views/product/15", "web/ie/6" => 2)

      Timecop.travel(15.minutes) #00:00:00
      subject.store("views/product/11", "web/ie/5" => 2)

      Timecop.travel(15.minutes) #00:00:15
      subject.store("views/product/11", "web/chrome/11" => 4)

      Timecop.travel(15.minutes) #00:00:30
      subject.store("views/product/11", "web/chrome/11" => 2)
    end

    it "is similar to redistat" do

      expect(subject.find("views/product", start_time, start_time + 55.minutes).total(:ever)).to eq("web" => 16.0,
        "web/chrome" => 6.0,
        "web/chrome/11" => 6.0,
        "web/firefox" => 3.0,
        "web/firefox/3" => 3.0,
        "web/ie" => 7.0,
        "web/ie/5" => 2.0,
        "web/ie/6" => 5.0)

      expect(subject.find("views/product", start_time, start_time + 55.minutes).total(:year)).to eq("web" => 16.0,
        "web/chrome" => 6.0,
        "web/chrome/11" => 6.0,
        "web/firefox" => 3.0,
        "web/firefox/3" => 3.0,
        "web/ie" => 7.0,
        "web/ie/5" => 2.0,
        "web/ie/6" => 5.0)

      expect(subject.find("views/product", start_time, start_time + 55.minutes).total).to eq('web' => 8,
        'web/firefox' => 3,
        'web/firefox/3' => 3,
        'web/ie' => 5,
        'web/ie/6' => 5)

      expect(subject.find("views/product/15", start_time, start_time + 55.minutes).points(:minute)).to eq([
        { moment: "2000-01-01T00:00", values: { "web/firefox" => 1, "web/firefox/3" => 1, "web" => 1 } },
        { moment: "2000-01-01T00:30", values: { "web" => 3, "web/ie/6" => 3, "web/ie" => 3 } },
        { moment: "2000-01-01T00:45", values: { "web" => 2, "web/ie/6" => 2, "web/ie" => 2 } }
      ])

      expect(subject.find("views/product/13", start_time, start_time + 55.minutes).points(:minute)).to eq([
        { moment: "2000-01-01T00:15", values: { "web/firefox" => 2, "web/firefox/3" => 2, "web" => 2 } },
      ])

      expect(subject.find("views/product", start_time, start_time + 55.minutes).points(:minute)).to eq([
        { moment: "2000-01-01T00:00", values: { "web/firefox" => 1, "web/firefox/3" => 1, "web" => 1 } },
        { moment: "2000-01-01T00:15", values: { "web/firefox" => 2, "web/firefox/3" => 2, "web" => 2 } },
        { moment: "2000-01-01T00:30", values: { "web" => 3, "web/ie/6" => 3, "web/ie" => 3 } },
        { moment: "2000-01-01T00:45", values: { "web" => 2, "web/ie/6" => 2, "web/ie" => 2 } }
      ])

      expect(subject.find("views", start_time, start_time + 55.minutes).points(:minute)).to eq([
        { moment: "2000-01-01T00:00", values: { "web/firefox" => 1, "web/firefox/3" => 1, "web" => 1 } },
        { moment: "2000-01-01T00:15", values: { "web/firefox" => 2, "web/firefox/3" => 2, "web" => 2 } },
        { moment: "2000-01-01T00:30", values: { "web" => 3, "web/ie/6" => 3, "web/ie" => 3 } },
        { moment: "2000-01-01T00:45", values: { "web" => 2, "web/ie/6" => 2, "web/ie" => 2 } }
      ])

      expect(subject.find("views", start_time).points(:hour)).to eq([
        {
          moment: "2000-01-01T00",
          values: {
            "web/ie" => 5.0,
            "web" => 8.0,
            "web/firefox" => 3.0,
            "web/ie/6" => 5.0,
            "web/firefox/3" => 3.0 }
        }, {
          moment: "2000-01-01T01",
          values: {
            "web/ie" => 2.0,
            "web/chrome" => 6.0,
            "web/chrome/11" => 6.0,
            "web" => 8.0,
            "web/ie/5" => 2.0
          }
        }
      ])
    end
    let(:filter) { Rhcf::Timeseries::Filter.new([:source, :browser], browser: 'firefox.*') }
    it "can find with filter" do
      expect(subject.find("views", start_time, start_time + 55.minutes, filter).points(:minute)).to eq([
        { moment: "2000-01-01T00:00", values: { "web/firefox" => 1, "web/firefox/3" => 1 } },
        { moment: "2000-01-01T00:15", values: { "web/firefox" => 2, "web/firefox/3" => 2 } },
      ])
    end
  end

  it "causes no stack overflow" do
    params_hash = {
      sender_domain: 'example.com',
      realm: 'realm',
      destination_domain: 'lvh.me',
      mail_server: 'aserver',
      bind_interface: '11.1.1.11'
    }

    {
      'sender_domain' => '%{sender_domain}',
      'realm_and_sender_domain' => '%{realm}/%{sender_domain}',
      'mail_server_and_interface' => '%{mail_server}/%{bind_interface}',
      'realm_and_destination_domain' => '%{realm}/%{destination_domain}',
      'destination_domain' => '%{destination_domain}'
    }.each do |known, unknown|
      subject.store(known % params_hash, [(unknown % params_hash), 'sent'].join('/') => 1)
    end
  end
end
