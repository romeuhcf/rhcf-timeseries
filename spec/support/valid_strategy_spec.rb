require 'redis'
require 'rhcf/timeseries/manager'
RSpec.shared_examples 'a valid strategy' do
  let(:redis){Redis.new}
  let(:manager) { Rhcf::Timeseries::Manager.new(connection: redis, strategy: described_class) }
  let(:start_time){  Time.parse("2000-01-01 00:00:00") }

  before do
    Timecop.travel(start_time)
    manager.store("views/product/15", {"web/firefox/3" => 1})

    Timecop.travel(15.minutes) #00:00:15
    manager.store("views/product/13", {"web/firefox/3" => 1}, Time.now)
    manager.store("views/product/13", {"web/firefox/3" => 1}, Time.now)
    manager.store("views/product/13", {"web/firefox/3" => 0}, Time.now)

    Timecop.travel(15.minutes) #00:00:30
    manager.store("views/product/15", {"web/ie/6" => 3})

    Timecop.travel(15.minutes) #00:00:45
    manager.store("views/product/15", {"web/ie/6" => 2})

    Timecop.travel(15.minutes) #00:00:00
    manager.store("views/product/11", {"web/ie/5" => 2})

    Timecop.travel(15.minutes) #00:00:15
    manager.store("views/product/11", {"web/chrome/11"=> 4})

    Timecop.travel(15.minutes) #00:00:30
    manager.store("views/product/11", {"web/chrome/11"=> 2})
  end

  it "is similar to redistat" do

    expect(manager.find("views/product", start_time, start_time + 55.minutes).total(:ever)).to  eq({
      "web" => 16.0,
      "web/chrome" => 6.0,
      "web/chrome/11" => 6.0,
      "web/firefox" => 3.0,
      "web/firefox/3" => 3.0,
      "web/ie" => 7.0,
      "web/ie/5" => 2.0,
      "web/ie/6" => 5.0
    })

    expect(manager.find("views/product", start_time, start_time + 55.minutes).total(:year)).to eq({
      "web" => 16.0,
      "web/chrome" => 6.0,
      "web/chrome/11" => 6.0,
      "web/firefox" => 3.0,
      "web/firefox/3" => 3.0,
      "web/ie" => 7.0,
      "web/ie/5" => 2.0,
      "web/ie/6" => 5.0
    })

    expect( manager.find("views/product", start_time, start_time + 55.minutes).total ).to eq({
      'web' => 8,
      'web/firefox' => 3,
      'web/firefox/3' => 3,
      'web/ie' => 5,
      'web/ie/6' => 5,
    })

    expect(manager.find("views/product/15", start_time, start_time + 55.minutes).points(:minute)).to eq([
      {:moment=>"2000-01-01T00:00", :values=>{"web/firefox"=>1, "web/firefox/3"=>1, "web"=>1}},
      {:moment=>"2000-01-01T00:30", :values=>{"web"=>3, "web/ie/6"=>3, "web/ie"=>3}},
      {:moment=>"2000-01-01T00:45", :values=>{"web"=>2, "web/ie/6"=>2, "web/ie"=>2}}
    ])

    expect(manager.find("views/product/13", start_time, start_time + 55.minutes).points(:minute)).to eq([
      {:moment=>"2000-01-01T00:15", :values=>{"web/firefox"=>2, "web/firefox/3"=>2, "web"=>2}},
    ])

    expect(manager.find("views/product", start_time, start_time + 55.minutes).points(:minute)).to eq([
      {:moment=>"2000-01-01T00:00", :values=>{"web/firefox"=>1, "web/firefox/3"=>1, "web"=>1}},
      {:moment=>"2000-01-01T00:15", :values=>{"web/firefox"=>2, "web/firefox/3"=>2, "web"=>2}},
      {:moment=>"2000-01-01T00:30", :values=>{"web"=>3, "web/ie/6"=>3, "web/ie"=>3}},
      {:moment=>"2000-01-01T00:45", :values=>{"web"=>2, "web/ie/6"=>2, "web/ie"=>2}}
    ])

    expect(manager.find("views", start_time, start_time + 55.minutes).points(:minute)).to eq([
      {:moment=>"2000-01-01T00:00", :values=>{"web/firefox"=>1, "web/firefox/3"=>1, "web"=>1}},
      {:moment=>"2000-01-01T00:15", :values=>{"web/firefox"=>2, "web/firefox/3"=>2, "web"=>2}},
      {:moment=>"2000-01-01T00:30", :values=>{"web"=>3, "web/ie/6"=>3, "web/ie"=>3}},
      {:moment=>"2000-01-01T00:45", :values=>{"web"=>2, "web/ie/6"=>2, "web/ie"=>2}}
    ])

    expect(manager.find("views", start_time).points(:hour)).to eq([
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

  let(:filter) { Rhcf::Timeseries::Filter.new([:source, :browser], browser: 'firefox.*' )}
  it "can find with filter" do

    expect(manager.find("views", start_time, start_time + 55.minutes, filter).points(:minute)).to eq([
      {:moment=>"2000-01-01T00:00", :values=>{"web/firefox"=>1, "web/firefox/3"=>1}},
      {:moment=>"2000-01-01T00:15", :values=>{"web/firefox"=>2, "web/firefox/3"=>2}},
    ])
  end
end
