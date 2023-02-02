# @param {String[]} words
# @param {String} order
# @return {Boolean}
def is_alien_sorted(words, order)
  # find first different char from words
  flag = nil
  order = order.chars.to_a
  s = 0
  n = 0
  while s < words.length - 1 do
    flag = nil
    w1 = words[s].chars.to_a
    w2 = words[s + 1].chars.to_a
    i = 0
    while i <= w1.length do
      if w1[i] != w2[i]
        if w1[i] == nil
          flag = true
        elsif w2[i] == nil
          flag = false
        elsif order.find_index(w1[i]) < order.find_index(w2[i])
          flag = true
        else
          flag = false
          break
        end
      end
      n += 1
      flag != nil ? break : nil
      i += 1
    end
    flag == false ? break : nil
    s += 1
  end
  flag == nil ? true : flag
end

puts is_alien_sorted(["apple","app"], "abcdefghijklmnopqrstuvwxyz")
