Dir[File.join(File.dirname(__FILE__), "support/**/*.rb")].each {|f| require f}
require 'timecop'
RSpec.configure do |config|
  config.run_all_when_everything_filtered = true
  config.filter_run :focus
  config.order = 'random'
end
