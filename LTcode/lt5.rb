nums = [2,7,11,15]
stash = {}
target = 9
nums.each_with_index do |num, i|
  diff = target - num
  stash.key?(diff) ? (return [stash[diff], i]) : stash[num] = i
end

