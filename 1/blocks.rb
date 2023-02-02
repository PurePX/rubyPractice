array = %w[Ann Mary Matt]
hash = { name: 'Dan', age: 26, height: 175 }

array.each { |name| print name }

5.times { print 'Hello Ruby' }

hash.each_key { |i| print i }

File.open('/block.txt', 'w') { |y| y.puts 'hello ruby' }
