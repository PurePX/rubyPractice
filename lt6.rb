# @param {Integer[]} nums
# @return {Integer}
def largest_perimeter(nums)
  n = nums.sort.reverse
  i = 0
  while i + 2 < n.size
    if n[i] < n[i + 1] + n[i + 2]
      puts n[i] + n[i + 1] + n[i + 2]
      break
    end
    i += 1
  end
  0
end

largest_perimeter([2, 1, 2, 4, 532, 543, 635, 67, 457, 455, 324, 32, 423, 4, 23, 4, 34, 32, 4, 3256, 436, 43, 634, 6,
                   325, 23, 4])
