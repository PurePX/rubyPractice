# @param {Integer} n
# @return {Boolean}
def is_happy(n)
  initial = n.dup
  initial = initial * initial
  total = 0
  flag = true
  until total == 1 do
    total = 0
    n.to_s.chars.to_a.each do |x|
      total += x.to_i * x.to_i
    end
    if total == 1 || total == 7
      flag = false
      break
    elsif n > 9

    elsif total == 1
      flag = true
      break
    end
    n = total
  end
  flag
end
puts is_happy(1)
