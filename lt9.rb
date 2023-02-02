# @param {Integer[]} arr
# @return {Boolean}
def can_make_arithmetic_progression(arr)
  arr.sort!
  can = true
  i = 0
  progr = arr[1] - arr[0]
  while i < arr.length - 1
    if arr[i+1] - progr != arr[i]
      can = false
      break
    end
  i += 1
  end
  can
end

puts can_make_arithmetic_progression([3,5,1])
