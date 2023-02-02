# @param {Integer} x
# @param {Integer} y
# @param {Integer[][]} points
# @return {Integer}
def nearest_valid_point(x, y, points)
  distances = []
  points.each_with_index do |point, index|
    a, b = point
    distances << [(x - a).abs + (y - b).abs, index] if a == x || b == y
  end
  return -1 if distances.empty?

  distances.sort.first.last
end
print nearest_valid_point(3, 4, [[1, 2], [3, 1], [2, 4], [2, 3], [4, 4]])
