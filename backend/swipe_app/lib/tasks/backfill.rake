namespace :swipe_app do
  namespace :report do
    desc "Show counts of cards, review_comments, and cards missing coordinates"
    task status: :environment do
      total_cards = Card.count
      comments = ReviewComment.count
      missing_coords = Card.where(latitude: nil).or(Card.where(longitude: nil)).count
      puts({ cards: total_cards, review_comments: comments, cards_missing_coords: missing_coords }.inspect)
    end
  end

  namespace :backfill do
    desc "Backfill latitude/longitude using Google Places Details API (use CONFIRM=1 to apply)"
    task :coords_from_place_id, [ :limit ] => :environment do |t, args|
      require "net/http"
      require "uri"
      require "json"

      api_key = ENV["GOOGLE_MAPS_API_KEY"]
      unless api_key
        puts "GOOGLE_MAPS_API_KEY is required"
        next
      end

      limit = (args[:limit] || 1000).to_i
      confirm = ENV["CONFIRM"] == "1"

      scope = Card.where.not(place_id: [ nil, "" ]).where(latitude: nil).or(
        Card.where.not(place_id: [ nil, "" ]).where(longitude: nil)
      ).order(:id).limit(limit)

      updated = 0
      processed = 0
      scope.find_each(batch_size: 50) do |card|
        processed += 1
        begin
          url = URI.parse("https://maps.googleapis.com/maps/api/place/details/json?place_id=#{URI.encode_www_form_component(card.place_id)}&fields=geometry&key=#{api_key}")
          res = Net::HTTP.start(url.host, url.port, use_ssl: true) do |http|
            http.read_timeout = 10
            http.open_timeout = 5
            http.get(url.request_uri)
          end
          body = JSON.parse(res.body) rescue {}
          if body["status"] == "OK" && body.dig("result", "geometry", "location")
            lat = body["result"]["geometry"]["location"]["lat"]
            lng = body["result"]["geometry"]["location"]["lng"]
            if lat && lng
              if confirm
                card.update_columns(latitude: lat, longitude: lng)
              end
              updated += 1
            end
          else
            warn "Failed for card ##{card.id} place_id=#{card.place_id} status=#{body['status']}"
          end
        rescue => e
          warn "Error for card ##{card.id}: #{e.class}: #{e.message}"
        ensure
          sleep 0.1 # rate limit
        end
      end

      if confirm
        puts "Updated #{updated}/#{processed} cards with coordinates"
      else
        puts "Would update ~#{updated}/#{processed} cards (dry-run). Use CONFIRM=1 to apply."
      end
    end
    desc "Create dummy review comments for cards (use CONFIRM=1 to actually write)"
    task :dummy_reviews, [ :per_card ] => :environment do |t, args|
      per_card = (args[:per_card] || 1).to_i
      confirm = ENV["CONFIRM"] == "1"
      phrases = [
        "雰囲気が良かったです。",
        "また来たいと思いました。",
        "コスパが高いと思います。",
        "清潔感がありました。",
        "接客が丁寧でした。"
      ]

      created = 0
      Card.find_each do |card|
        next if ReviewComment.where(card_id: card.id).exists?
        if confirm
          per_card.times do
            ReviewComment.create!(card_id: card.id, comment: phrases.sample)
            created += 1
          end
        else
          created += per_card
        end
      end

      puts "Would create #{created} review_comments (use CONFIRM=1 to apply)" unless confirm
      puts "Created #{created} review_comments" if confirm
    end
  end
end
