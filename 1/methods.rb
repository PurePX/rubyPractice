def division
  print 'method body'
end

def division2(name)
  print 'My name is ' + name
end

def division3
  name = 'Abygail'
  division
  division2(name)
end

division3
print 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'

def division4(name = '', age = 20, _weight = '')
  var = 'Hello '
  var += 'World' if name != 'Ann' and age == 20
  # puts 'My name is ' + name
  # puts 'My age is ' + age.to_s
  # puts 'My weight is ' + "#{weight}"
  var
end

print division4 'Aby', 20, 70
print division4 'Ann', 20, 78
print division4 'Michael', 20, 120
print division4 'Ron', 2, 14
