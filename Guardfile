guard :rspec, cmd: "NO_COVERAGE=true bundle exec rspec", failed_mode: :keep, all_after_pass: true, all_on_start: true do

  watch(%r{^(spec/.+_spec\.rb)$}) { |m| m[1] }
  watch(%r{^lib/(.+)\.rb$}) { |m| "spec/lib/#{m[1]}_spec.rb" }
  watch('spec/spec_helper.rb')  { "spec" }
  watch(%r{^spec/support/(.+)\.rb$})  { "spec" }
end

guard :bundler do
  watch('Gemfile')
  # Uncomment next line if your Gemfile contains the `gemspec' command.
  # watch(/^.+\.gemspec/)
end
