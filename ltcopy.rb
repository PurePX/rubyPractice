arr = "hlabcdefgijkmnopqrstuvwxyz"
words = ["hello","leetcode"]
puts words.sort_by{|x| x.length.reverse}
arr = arr.chars.to_a
puts arr.find_index('m')

