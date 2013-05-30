require './edit_distance'

class SpellingSuggestion
  def initialize(term, dictionary)
    @term = term
    @dictionary = dictionary.reject{ |t| t.nil? || t.length == 0 }
  end

  def result
    @dictionary.map do |dict_term|
      [dict_term, EditDistance.new(@term, dict_term).result]
    end.sort{ |a, b| a[1] <=> b[1] }.first[0]
  end
end