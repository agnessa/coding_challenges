require './spelling_suggestion'

f = File.open(ARGV[0])
lines = f.readlines.map(&:chomp)
f.close

test_case_count = lines.shift.to_i
lines.shift
test_cases = Array.new(test_case_count)

lines.each_with_index.map do |l, idx|
  test_cases[idx / 4] ||= []
  test_cases[idx / 4] << l
end

test_cases.each do |tc|
  puts SpellingSuggestion.new(tc.shift, tc).result
end 
