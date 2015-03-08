# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'rhcf/timeseries/version'

Gem::Specification.new do |spec|
  spec.name          = "rhcf-timeseries"
  spec.version       = Rhcf::Timeseries::VERSION
  spec.authors       = ["Romeu Fonseca"]
  spec.email         = ["romeu.hcf@gmail.com"]
  spec.summary       = %q{Redistat inspired redis time series.}
  spec.description   = %q{Gem to allow your system to keep record of time series on rhcf}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "redis"
  spec.add_development_dependency "rspec"
  #spec.add_development_dependency "guard"
  #spec.add_development_dependency "guard-rspec"
  #spec.add_development_dependency "guard-bundler"
  #spec.add_development_dependency "simplecov"
  spec.add_development_dependency "timecop"
  spec.add_development_dependency "stackprof"
  #spec.add_dependency "activesupport"
end
