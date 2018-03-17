require 'nokogiri'
require 'csv'

@id_gen = 1

def attr(path)
  @items.xpath(path).first&.text
end

movies = {}
people = {}
people_movies = []

Dir['./movie/*'].each do |n|

  path = File.expand_path(n)

  xml = File.open(path) { |f| Nokogiri::XML(f) }
  xml.remove_namespaces!
  @items = xml.xpath('//ItemLookupResponse/Items/Item').first

  if @items.nil?
    p n
    next
  end

  asin = attr('//Item/ASIN')
  page_url = attr('//Item/DetailPageURL')
  image_url = attr('//Item/LargeImage/URL')
  audience_rating = attr('//ItemAttributes/AudienceRating')
  movie_binding = attr('//ItemAttributes/Binding')
  genre = attr('//ItemAttributes/Genre')
  is_adult = attr('//ItemAttributes/IsAdultProduct').to_i
  prod_group = attr('//ItemAttributes/ProductGroup')
  prod_type = attr('//ItemAttributes/ProductTypeName')
  time = attr('//ItemAttributes/RunningTime').to_i
  studio = attr('//ItemAttributes/Studio')
  title = attr('//ItemAttributes/Title')
  release_date = attr('//ItemAttributes/ReleaseDate')

  movies[asin] = [
    page_url, image_url, audience_rating, movie_binding,
    genre, is_adult, prod_group, prod_type, time, studio,
    title, release_date
  ]

  @items.xpath('//ItemAttributes/Director').each do |node|
    name = node.text
    if person_id = people[name]
      people_movies.push([person_id, asin, 'director'])
    else
      people[name] = @id_gen
      people_movies.push([@id_gen, asin, 'director'])
      @id_gen += 1
    end
  end

  @items.xpath('//ItemAttributes/Creator').each do |node|
    role = node.attributes['Role'].value
    name = node.text
    if person_id = people[name]
      people_movies.push([person_id, asin, role])
    else
      people[name] = @id_gen
      people_movies.push([@id_gen, asin, role])
      @id_gen += 1
    end
  end

  @items.xpath('//ItemAttributes/Actor').each do |actor|
    name = actor.text
    if person_id = people[name]
      people_movies.push([person_id, asin, 'actor'])
    else
      people[name] = @id_gen
      people_movies.push([@id_gen, asin, 'actor'])
      @id_gen += 1
    end
  end

end

p movies.count
p people.count
p people_movies.count

stamp = Time.now.to_i

CSV.open("movies_#{stamp}.csv", 'w') do |csv|
  movies.each do |k, v|
    csv << ([k] + v)
  end
end

CSV.open("people_#{stamp}.csv", 'w') do |csv|
  people.each do |k, v|
    csv << [v, k]
  end
end

CSV.open("people_movies_#{stamp}.csv", 'w') do |csv|
  people_movies.each do |pm|
    csv << pm
  end
end
