# @param {Integer[]} nums
# @return {Integer}
def array_sign(nums)
  prod = 1
  nums.each{|n| prod *= n}
  case
  when prod > 0
    return 1
  when prod < 0
    return -1
  else
    return 0
  end
end

puts array_sign([-1,-2,-3,-4,3,2,1])
