# @param {Integer[][]} coordinates
# @return {Boolean}
def check_straight_line(coordinates)
  flag = true
  coordinates.sort!
  print coordinates
  coordinates.each_with_index do |n, index|
    arr = []
    #straight line
    if coordinates[0][0] == coordinates[1][0]
      arr << n[1] + 1
      if coordinates[index + 1] == nil
        break
      elsif arr[0] == coordinates[index + 1][1]
        nil
      else
        flag = false
        break
      end
    elsif coordinates[0][1] == coordinates[1][1]
      arr << n[0] + 1
      if coordinates[index + 1] == nil
        break
      elsif arr[0] == coordinates[index + 1][0]
        nil
      else
        flag = false
        break
      end
    else
      #non straight line
      arr << n[0] + 1
      arr << n[1] + 1
      if coordinates[index + 1] == nil
        break
      elsif arr == coordinates[index + 1]
        nil
      else
        flag = false
        break
      end
    end
  end
  flag
end
puts check_straight_line([[2,1],[4,2],[6,3]])
