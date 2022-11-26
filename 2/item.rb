class Item
  attr_accessor :price, :weight, :height

  def initialize(options = {})
    @price = options[:price]
    @weight = options[:weight]
  end
end
