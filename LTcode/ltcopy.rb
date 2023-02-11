# @param {String} s1
# @param {String} s2
# @return {Boolean}
def check_inclusion(s1, s2)
  long = s2.chars.to_a
  short = s1.chars.to_a
  long.each_with_index do |n, index|
    start = 0
    if short.empty?
      break
    elsif short.include?(n)
      start = index
      short.delete_at(short.index(n))
    else
      short = s1.chars.to_a
    end
  end

  if short.empty?
    true
  else
    long = s2.chars.to_a.reverse
    short = s1.chars.to_a
    long.each do |n|
      if short.empty?
        break
      elsif short.include?(n)
        short.delete_at(short.index(n))
      else
        short = s1.chars.to_a
      end
    end
  end
  short.empty? ? true : false
end
puts check_inclusion("adc", "dcda")


