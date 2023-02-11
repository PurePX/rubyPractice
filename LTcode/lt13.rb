# @param {String} s1
# @param {String} s2
# @return {Boolean}
def check_inclusion(s1, s2)
  long = s2.chars.to_a
  short = s1.chars.to_a
  n = 0
  while n < long.length do
    if short.empty?
      break
    elsif short.include?(long[n])
      start = n
      while n < long.length do
        if short.include?(long[n])
          short.delete_at(short.index(long[n]))
          n += 1
          break if short.empty?
        else
          short = s1.chars.to_a
          n = start + 1
          break
        end
      end
    else
      n += 1
    end
  end
  short.empty? ? true : false
end
puts check_inclusion("ab", "eidboaoo")


