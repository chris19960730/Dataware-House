require 'csv'

path = File.expand_path('./review-all.txt')
stamp = Time.now.to_i
output_filename = "review_csv_#{stamp}.csv"

CSV.open(output_filename, 'w') do |csv|
  File.foreach(path).with_index do |str, line_num|
    result = str.match(%r{product/productId: (\w+) review/userId: (.+) review/profileName: (.*)})
    @prod_id = result[1].strip
    @user_id = result[2].strip
    rest1 = result[3].split(' review/helpfulness: ')
    @profile_name = rest1[0].strip
    rest2 = rest1[1].match(%r{(\d+)/(\d+) review/score: ([0-9.]+) review/time: (\d+)(.*)})
    @helpful = rest2[1].to_i
    @helpful_all = rest2[2].to_i
    @score = rest2[3].to_f
    @ctime = rest2[4].to_i

    rest3 = rest2[5].strip.match(%r{review/summary: (.*)})
    if rest3.nil?
      @summary = @text = ''
    else
      rest4 = rest3[1].split(' review/text: ')
      @summary = rest4[0]
      @text = rest4[1]&.strip || ''
    end

    csv << [line_num + 1, @prod_id, @user_id, @profile_name, @helpful, @helpful_all, @score, @ctime, @summary, @text]
  end
end
