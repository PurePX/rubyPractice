def division
  puts  "method body"
end

def division2(name)
  puts "My name is " + name
end

def division3
  name = 'Abygail'
  division
  division2(name)
end

division3
puts 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'


def division4(name = '', age = 20, weight = '')
  var = 'Hello '
  if name != 'Ann' and age == 20
    var += 'World'
  end
  # puts 'My name is ' + name
  # puts 'My age is ' + age.to_s
  # puts 'My weight is ' + "#{weight}"
  return var
end

puts division4 'Aby', 20, 70
puts division4 'Ann', 20, 78
puts division4 'Michael', 20, 120
puts division4 'Ron', 2, 14
