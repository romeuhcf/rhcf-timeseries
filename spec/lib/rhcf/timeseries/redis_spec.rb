require 'spec_helper'
require 'timecop'
require 'redis'
require 'micon'
require 'rhcf/timeseries/redis'
require 'benchmark'
require 'logger'
micon.register(:logger){Logger.new('/dev/null')}
#micon.register(:logger){Logger.new(STDOUT)}
micon.register(:redis_connection){Redis.new}


describe Rhcf::Timeseries::Redis do

  before(:each) do
    Timecop.return
    subject.flush!
  end

  it "should be fast to store and read" do
    subject.flush!
    total = 0
    start_time = Time.now
    
    bench = Benchmark.measure {
        10000.times do 
          total +=1
          subject.store("a", {"b" => 1} ) #, time)
        end
    }

    
#pp    subject.find("a", start_time - 11100, Time.now + 11100).points(:second)
    qbench = Benchmark.measure {
      subject.find("a", start_time - 11100, Time.now + 11100).total['b'].to_i.should == total
    }

    qbench_year = Benchmark.measure {
      subject.find("a", start_time - 100000, Time.now + 100000).total(:year)['b'].to_i.should == total
    }
    
    puts "Write speed %d points/seg | points:%d, duration:%0.3fs | query_time %0.3fs" % [speed = (1.0 * total / (bench.total + 0.00000001)), total, bench.total, qbench.total]
    speed.should > 400
  end


  it "should be similar to redistat" do
    start_time = Time.parse("2000-01-01 00:00:00")
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

    subject.find("views/product", start_time, start_time + 55.minutes).total(:ever).should == {
     "web" => 16.0,
      "web/chrome" => 6.0,
      "web/chrome/11" => 6.0,
      "web/firefox" => 3.0,
      "web/firefox/3" => 3.0,
      "web/ie" => 7.0,
      "web/ie/5" => 2.0,
      "web/ie/6" => 5.0
    }

    subject.find("views/product", start_time, start_time + 55.minutes).total(:year).should == {
     "web" => 16.0,
      "web/chrome" => 6.0,
      "web/chrome/11" => 6.0,
      "web/firefox" => 3.0,
      "web/firefox/3" => 3.0,
      "web/ie" => 7.0,
      "web/ie/5" => 2.0,
      "web/ie/6" => 5.0
    }

    subject.find("views/product", start_time, start_time + 55.minutes).total.should == {
      'web' => 8,
      'web/firefox' => 3,
      'web/firefox/3' => 3,
      'web/ie' => 5,
      'web/ie/6' => 5,
    }

    subject.find("views/product/15", start_time, start_time + 55.minutes).points(:minute).should == [
      {:moment=>"2000-01-01T00:00", :values=>{"web/firefox"=>1, "web/firefox/3"=>1, "web"=>1}},
      {:moment=>"2000-01-01T00:30", :values=>{"web"=>3, "web/ie/6"=>3, "web/ie"=>3}},
      {:moment=>"2000-01-01T00:45", :values=>{"web"=>2, "web/ie/6"=>2, "web/ie"=>2}}
    ]

    subject.find("views/product/13", start_time, start_time + 55.minutes).points(:minute).should == [
      {:moment=>"2000-01-01T00:15", :values=>{"web/firefox"=>2, "web/firefox/3"=>2, "web"=>2}},
    ]




    subject.find("views/product", start_time, start_time + 55.minutes).points(:minute).should == [
      {:moment=>"2000-01-01T00:00", :values=>{"web/firefox"=>1, "web/firefox/3"=>1, "web"=>1}},
      {:moment=>"2000-01-01T00:15", :values=>{"web/firefox"=>2, "web/firefox/3"=>2, "web"=>2}},
      {:moment=>"2000-01-01T00:30", :values=>{"web"=>3, "web/ie/6"=>3, "web/ie"=>3}},
      {:moment=>"2000-01-01T00:45", :values=>{"web"=>2, "web/ie/6"=>2, "web/ie"=>2}}
    ]

    subject.find("views", start_time, start_time + 55.minutes).points(:minute).should == [
      {:moment=>"2000-01-01T00:00", :values=>{"web/firefox"=>1, "web/firefox/3"=>1, "web"=>1}},
      {:moment=>"2000-01-01T00:15", :values=>{"web/firefox"=>2, "web/firefox/3"=>2, "web"=>2}},
      {:moment=>"2000-01-01T00:30", :values=>{"web"=>3, "web/ie/6"=>3, "web/ie"=>3}},
      {:moment=>"2000-01-01T00:45", :values=>{"web"=>2, "web/ie/6"=>2, "web/ie"=>2}}
    ]

    subject.find("views", start_time).points(:hour).should == [{:moment=>"2000-01-01T00",
         :values=>
          {"web/ie"=>5.0,
           "web"=>8.0,
           "web/firefox"=>3.0,
           "web/ie/6"=>5.0,
           "web/firefox/3"=>3.0}},
        {:moment=>"2000-01-01T01",
         :values=>
          {"web/ie"=>2.0,
           "web/chrome"=>6.0,
           "web/chrome/11"=>6.0,
           "web"=>8.0,
           "web/ie/5"=>2.0}}]


  end
end
