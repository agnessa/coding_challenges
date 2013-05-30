require './spelling_suggestion'

describe SpellingSuggestion, "#result" do
  it "returns remembrance for remimance" do
  	ss = SpellingSuggestion.new('remimance', ['remembrance', 'reminiscence'])
    ss.result.should eq('remembrance')
  end
  it "returns remembrance for remembrance" do
  	ss = SpellingSuggestion.new('remembrance', ['remembrance', 'reminiscence'])
    ss.result.should eq('remembrance')
  end

end 