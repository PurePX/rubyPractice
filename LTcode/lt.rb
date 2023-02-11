def min_stickers(stickers, target)
  letter_count = Hash.new(0)
  stickers_count = Hash.new(0)
  target.each_char do |ch|
      letter_count[ch] += 1
  end

  stickers.each do |sticker|
      sticker.each_char do |ch|
          stickers_count[ch] += 1
      end
  end

  ans = 0
  letter_count.keys.each do |ch|
      if stickers_count[ch] == 0
          return -1
      end
      ans += (letter_count[ch] + stickers_count[ch] - 1) / stickers_count[ch]
  end
  ans
end

stickers = ["travel","quotient","nose","wrote","any"]
target = "lastwest"
puts min_stickers(stickers, target)
