require_relative 'cart'
require_relative 'item'

item = Item.new({ price: 25, weight: 120, name: 'Car' })
item1 = Item.new({ price: 25, weight: 120, name: 'Car' })

item.info { |value| print value }
item1.info
