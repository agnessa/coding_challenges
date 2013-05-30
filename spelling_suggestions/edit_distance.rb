class EditDistance
  def initialize(term1, term2)
    @term1 = term1
    @term2 = term2
  end

  def result
    # thank you wikipedia - ruby translation of the pseudocode provided
    # http://en.wikipedia.org/wiki/Levenshtein_distance

    # degenerate cases
    return 0 if @term1 == @term2
    return @term2.length if @term1.length == 0
    return @term1.length if @term2.length == 0
 
    # create two work vectors of integer distances
    
    # initialize v0 (the previous row of distances)
    # this row is A[0][i]: edit distance for an empty s
    # the distance is just the number of characters to delete from t
    v0 = (0...@term2.length).to_a
    v1 = Array.new(@term2.length)
 
    for i in 0...@term1.length
        # calculate v1 (current row distances) from the previous row v0
 
        # first element of v1 is A[i+1][0]
        #   edit distance is delete (i+1) chars from s to match empty t
        v1[0] = i + 1;
 
        # use formula to fill in the rest of the row
        for j in 0...@term2.length - 1
          cost = ((@term1[i] == @term2[j]) ? 0 : 1)
          v1[j + 1] = [v1[j] + 1, v0[j + 1] + 1, v0[j] + cost].min
        end
 
        # copy v1 (current row) to v0 (previous row) for next interation
        for j in 0...v0.length
          v0[j] = v1[j]
        end
    end

    v1[@term2.length - 1]
  end
end