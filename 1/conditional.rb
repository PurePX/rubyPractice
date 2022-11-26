if 2 > 1
    puts '2 > 1'
else
    puts 'no'
end

if 2 < 1
    puts '2 < 1'
else
    puts 'no'
end

puts 'yes' unless 2 < 1
puts 'yes' if 2 > 1

name = 'Mary'

if name == 'Angel'
    puts name
elsif name == 'Aby'
    puts name
elsif name == 'Mary'
    puts name
elsif name == 'Ann'
    puts name
end

if name == 'Mary' and 1 < 2 
    puts name + '1'
end
