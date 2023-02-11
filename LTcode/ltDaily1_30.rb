n = 0
t = [0, 1, 1]
if n < 3
  return t[n]
else
  i = 0
  while i < n - 2 do
    t << (t[t.length - 3] + t[t.length - 2] + t[t.length - 1])
    i += 1
  end
  return t[t.length - 1]
end
