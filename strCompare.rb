a = "abc"
b = "abcd"
c = "abcd"

puts "--------------"
puts a > b
puts a = b
puts a < b
puts b > a
puts b < a
puts c >= b
puts c <= b
puts c <= "abcde"
puts c <= "abc"
puts "c" <= "abcde"
