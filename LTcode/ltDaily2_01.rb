# @param {String} str1
# @param {String} str2
# @return {String}
def gcd_of_strings(str1, str2)
  gcd = str1.length.gcd(str2.length)
  until gcd == 0 do
    i = 0
    found = false
    long = ''
    decompose = str1 <= str2 ?  str1 : str2
    decompose == str1 ? long = str2 : long = str1
    check = ''
    #retrieving first gcd symb from short word
    until decompose.length == 0 do
      check = decompose[...gcd]
      n = 1
      #compose long word from gcd
      while n <= long.length + 1
        i = 1
        #also compose short word
        if check*n == long
          x = 1
          while x <= decompose.length + 1
            check*x == decompose ? found = true : nil
            x += 1
            #found? yay!
            if found == true
              break
            end
          end
        end
        #exit loop again
        if found == true
          break
        end
        n += 1
      end
      #exit loop again
      if found == true
        break
      end

      #if all fails try next check combintaion (add symbol)
      decompose = decompose[1..-1]
    end
    if found == true
      return check
      break
    end
    gcd -= 1
  end
  if found == false
    return ""
  end

end
puts gcd_of_strings('AA', 'A')
