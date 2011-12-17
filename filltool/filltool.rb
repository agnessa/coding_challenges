#!/usr/bin/ruby

class DisjointSetElement
  attr_accessor :parent, :rank

  def initialize
    @parent = nil
    @rank = 0
  end

  def find
    element = self
    until element.parent.nil?
      element = element.parent
    end
    element
  end

  def union(other)
    my_root = self.find
    other_root = other.find
    return self if my_root == other_root
    if my_root.rank > other_root.rank
      other_root.parent = my_root
    elsif my_root.rank < other_root.rank
      my_root.parent = other_root
    else
      other_root.parent = my_root
      my_root.rank += 1
    end
    self
  end
end

class ConnectedSegmentExtractor
  def initialize(filename)
    line_no = -1
    @rows, @cols, @data = nil, nil, nil
    File.open(filename).each_line do |line|
      if line_no < 0
        (@rows, @cols) = line.split.map(&:to_i)
        @data = Array.new(@rows)
      else
	@data[line_no] = Array.new(@cols)
        line.chomp.split(//).map(&:to_i).each_with_index do |c, i|
          @data[line_no][i] = (c==1 ? 0 : 1) #invert colors, 0 is now white (background)
        end
      end
      line_no += 1
    end
  end

  #returns connected elements i.e. ne, n, nw and w neighbours
  def get_neighbours(row_no, col_no)
    neighbours = []
    if row_no > 0
      #ne
      neighbours<< @data[row_no - 1][col_no + 1] if col_no < (@cols - 1)
      #n
      neighbours<< @data[row_no - 1][col_no]
      #nw
      neighbours<< @data[row_no - 1][col_no - 1] if col_no > 0
    end
    #w
    neighbours<< @data[row_no][col_no - 1] if col_no > 0
    neighbours.reject{ |e| e == 0 }.uniq
  end

  def add_labels
    @equi_labels = {}
    next_label = 1
    @rows.times do |row_no|
      @cols.times do |col_no|
        ele = @data[row_no][col_no]
        #If the element is not the background
        next if ele == 0
        
        #1. Get the neighbouring elements of the current element
        neighbours = get_neighbours(row_no, col_no)
        #2. If there are no neighbours, uniquely label the current element and continue
        if neighbours.empty?
          @equi_labels[next_label] = DisjointSetElement.new
          @data[row_no][col_no] = next_label
          next_label += 1
          next
        end
        #3. Otherwise, find the neighbour with the smallest label and assign it to the current element
        @data[row_no][col_no] = neighbours.min
        #4. Store the equivalence between neighbouring labels
        neighbours.each do |label|
          @equi_labels[label].union(@equi_labels[@data[row_no][col_no]])
        end
      end   
    end
  end

  def count
    add_labels
    tmp = @equi_labels.keys.map do |label|
      @equi_labels[label].find.object_id
    end
    tmp.uniq.size
  end
end

cse = ConnectedSegmentExtractor.new(ARGV[0])
puts cse.count

