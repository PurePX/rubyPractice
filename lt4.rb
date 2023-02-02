n = 234
dig = n.to_s.chars.to_a.map(&:to_i)
puts dig.inject(:*) - dig.sum
