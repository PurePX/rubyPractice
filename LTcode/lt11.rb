# @param {String} s1
# @param {String} s2
# @return {Boolean}
def are_almost_equal(s1, s2)
  s1 = s1.chars.to_a
  s2 = s2.chars.to_a
  diff1 = []
  diff2 = []
  i = 0
  if s1 == s2
    true
  else
    while i < s1.length
      s1[i] == s2[i] ?  nil : diff1 << s1[i]
      s2[i] == s1[i] ?  nil : diff2 << s2[i]
      i += 1
    end
    if diff1.length == 2 && diff1.sort == diff2.sort
      true
    else
      false
    end
  end
end

puts are_almost_equal('caa', 'aaz')
