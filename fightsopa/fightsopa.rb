#!/usr/bin/ruby

substitutions = {}
cipher_line = true
File.open(ARGV[0]).each_line do |line|
  if cipher_line
    cipher = line.to_i % 26
    letters = %w{a b c d e f g h i j k l m n o p q r s t u v w x y z}
    letters.each_with_index do |letter, i|
      substitutions[letter] = letters[i-cipher]
      substitutions[letter.upcase] = letters[i-cipher].upcase
    end
    cipher_line = false
  else
    puts line.split(//).map{|char| substitutions[char] || char}.join('')
  end
end


