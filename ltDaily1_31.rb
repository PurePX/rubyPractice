# @param {Integer[]} scores
# @param {Integer[]} ages
# @return {Integer}
def best_team_score(scores, ages)
  table = []
  i = 0
  maxsum = 0

  while i < scores.length
    table << [scores[i], ages[i]]
    i += 1
  end
  i = 0
  table.sort_by! { |player| player[1] }.reverse!
  while i < scores.length
    roster = []

    table.each do |player|
      good = true
      ages1 = roster.select { |team| team[1] >= player[1] }
      if roster == []
        roster << player
        good = false
      elsif ages1 != []
        roster.each do |team|

          player[0] > team[0] ? good = false : nil
          player[1] > team[1] ? good = false : nil
          player[1] == team[1] ? good = true : nil
        end
      else
        roster.each do |team|
          team[0] >= player[0] ? good = false : nil
        end
      end
      roster << player if good == true
    end
    print roster
    puts 'a'
    i += 1
    sum = []
    roster.each { |player| sum << player[0] }
    maxsum = sum.sum if sum.sum > maxsum
    table = table.rotate(1)

  end
  maxsum
end

puts best_team_score([1,3,7,3,2,4,10,7,5], [4,5,2,1,1,2,4,1,4])
