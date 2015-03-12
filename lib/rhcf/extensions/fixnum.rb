class Fixnum
  def minutes
    self * 60
  end

  def hours
    self.minutes * 60
  end

  def days
    self.hours * 24
  end

  def seconds
    self
  end

  def weeks
    self.days * 7
  end

  def years
    self.days * 365
  end

  alias_method :day, :days
  alias_method :week, :weeks
  alias_method :hour, :hours
  alias_method :second, :seconds
  alias_method :minute, :minutes
  alias_method :year, :years
end


