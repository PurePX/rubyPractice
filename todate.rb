require 'date'

dos = Date.new(2010, 1, 1)
puts 'тип данных dos = ' + dos.class.to_s
puts dos

days = 5
puts 'тип данных days = ' + days.class.to_s
puts days

newDos = dos + days # Прибавляем int к Date
puts '------------------'
puts newDos
