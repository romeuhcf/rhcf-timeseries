require 'spec_helper'
require 'timecop'
require 'redis'
require 'rhcf/timeseries/redis'
require 'benchmark'
require 'stackprof'

describe Rhcf::Timeseries::Redis do
  let(:redis_connection){Redis.new}
  subject{Rhcf::Timeseries::Redis.new(redis_connection)}

  before(:each) do
    Timecop.return
    subject.flush!
  end

  describe 'descending' do
    it "is be fast to store and read" do
      total = 0
      start_time = Time.now

      bench = Benchmark.measure {
        StackProf.run(mode: :cpu, out: p('/tmp/stackprof-cpu-store-descend.dump')) do
          1000.times do
            total +=1
            subject.store("a/b", {"e/f" => 1} ) #, time)
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
            total +=1
            subject.store("a/b/c/d", {"e/f/g/h" => 1}  , Time.now, false, false)
          end
        end
      }

      puts "No descend write speed %d points/seg | points:%d, duration:%0.3fs" % [new_speed = (1.0 * total / (bench.total + 0.00000001)), total, bench.total]
      expect(new_speed).to be > 300
    end
  end

  describe "find and total" do
    let(:start_time){  Time.parse("2000-01-01 00:00:00") }
    before do
      Timecop.travel(start_time)
      subject.store("views/product/15", {"web/firefox/3" => 1})

      Timecop.travel(15.minutes) #00:00:15
      subject.store("views/product/13", {"web/firefox/3" => 1}, Time.now)
      subject.store("views/product/13", {"web/firefox/3" => 1}, Time.now)
      subject.store("views/product/13", {"web/firefox/3" => 0}, Time.now)

      Timecop.travel(15.minutes) #00:00:30
      subject.store("views/product/15", {"web/ie/6" => 3})

      Timecop.travel(15.minutes) #00:00:45
      subject.store("views/product/15", {"web/ie/6" => 2})

      Timecop.travel(15.minutes) #00:00:00
      subject.store("views/product/11", {"web/ie/5" => 2})

      Timecop.travel(15.minutes) #00:00:15
      subject.store("views/product/11", {"web/chrome/11"=> 4})

      Timecop.travel(15.minutes) #00:00:30
      subject.store("views/product/11", {"web/chrome/11"=> 2})
    end

    it "is similar to redistat" do

      expect(subject.find("views/product", start_time, start_time + 55.minutes).total(:ever)).to  eq({
        "web" => 16.0,
        "web/chrome" => 6.0,
        "web/chrome/11" => 6.0,
        "web/firefox" => 3.0,
        "web/firefox/3" => 3.0,
        "web/ie" => 7.0,
        "web/ie/5" => 2.0,
        "web/ie/6" => 5.0
      })

      expect(subject.find("views/product", start_time, start_time + 55.minutes).total(:year)).to eq({
        "web" => 16.0,
        "web/chrome" => 6.0,
        "web/chrome/11" => 6.0,
        "web/firefox" => 3.0,
        "web/firefox/3" => 3.0,
        "web/ie" => 7.0,
        "web/ie/5" => 2.0,
        "web/ie/6" => 5.0
      })

      expect( subject.find("views/product", start_time, start_time + 55.minutes).total ).to eq({
        'web' => 8,
        'web/firefox' => 3,
        'web/firefox/3' => 3,
        'web/ie' => 5,
        'web/ie/6' => 5,
      })

      expect(subject.find("views/product/15", start_time, start_time + 55.minutes).points(:minute)).to eq([
        {:moment=>"2000-01-01T00:00", :values=>{"web/firefox"=>1, "web/firefox/3"=>1, "web"=>1}},
        {:moment=>"2000-01-01T00:30", :values=>{"web"=>3, "web/ie/6"=>3, "web/ie"=>3}},
        {:moment=>"2000-01-01T00:45", :values=>{"web"=>2, "web/ie/6"=>2, "web/ie"=>2}}
      ])

      expect(subject.find("views/product/13", start_time, start_time + 55.minutes).points(:minute)).to eq([
        {:moment=>"2000-01-01T00:15", :values=>{"web/firefox"=>2, "web/firefox/3"=>2, "web"=>2}},
      ])

      expect(subject.find("views/product", start_time, start_time + 55.minutes).points(:minute)).to eq([
        {:moment=>"2000-01-01T00:00", :values=>{"web/firefox"=>1, "web/firefox/3"=>1, "web"=>1}},
        {:moment=>"2000-01-01T00:15", :values=>{"web/firefox"=>2, "web/firefox/3"=>2, "web"=>2}},
        {:moment=>"2000-01-01T00:30", :values=>{"web"=>3, "web/ie/6"=>3, "web/ie"=>3}},
        {:moment=>"2000-01-01T00:45", :values=>{"web"=>2, "web/ie/6"=>2, "web/ie"=>2}}
      ])

      expect(subject.find("views", start_time, start_time + 55.minutes).points(:minute)).to eq([
        {:moment=>"2000-01-01T00:00", :values=>{"web/firefox"=>1, "web/firefox/3"=>1, "web"=>1}},
        {:moment=>"2000-01-01T00:15", :values=>{"web/firefox"=>2, "web/firefox/3"=>2, "web"=>2}},
        {:moment=>"2000-01-01T00:30", :values=>{"web"=>3, "web/ie/6"=>3, "web/ie"=>3}},
        {:moment=>"2000-01-01T00:45", :values=>{"web"=>2, "web/ie/6"=>2, "web/ie"=>2}}
      ])

      expect(subject.find("views", start_time).points(:hour)).to eq([
        {
          :moment=>"2000-01-01T00",
          :values=> {
            "web/ie"=>5.0,
            "web"=>8.0,
            "web/firefox"=>3.0,
            "web/ie/6"=>5.0,
            "web/firefox/3"=>3.0}
        },{
          :moment=>"2000-01-01T01",
          :values=>{
            "web/ie"=>2.0,
            "web/chrome"=>6.0,
            "web/chrome/11"=>6.0,
            "web"=>8.0,
            "web/ie/5"=>2.0
          }
        }
      ])
    end
    it "can find with filter" do
      expect(subject.find("views", start_time, start_time + 55.minutes, /firefox/).points(:minute)).to eq([
        {:moment=>"2000-01-01T00:00", :values=>{"web/firefox"=>1, "web/firefox/3"=>1}},
        {:moment=>"2000-01-01T00:15", :values=>{"web/firefox"=>2, "web/firefox/3"=>2}},
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
      subject.store(known % params_hash, {[(unknown % params_hash),'sent'].join('/') => 1})
    end
  end
end
