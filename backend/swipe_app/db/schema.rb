# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[8.0].define(version: 2025_08_23_060000) do
  create_table "cards", charset: "utf8mb4", collation: "utf8mb4_0900_ai_ci", force: :cascade do |t|
    t.string "genre", limit: 32
    t.string "title", limit: 128, null: false
    t.float "rating", default: 0.0, null: false
    t.integer "review_count", default: 0, null: false
    t.string "image_url", limit: 1000
    t.string "external_link", limit: 256
    t.string "region", limit: 16
    t.string "address", limit: 128
    t.decimal "latitude", precision: 10, scale: 8
    t.decimal "longitude", precision: 11, scale: 8
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.string "place_id", limit: 128
    t.index ["place_id"], name: "index_cards_on_place_id", unique: true
  end

  create_table "review_comments", charset: "utf8mb4", collation: "utf8mb4_0900_ai_ci", force: :cascade do |t|
    t.text "comment"
    t.bigint "card_id", null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["card_id"], name: "index_review_comments_on_card_id"
  end

  create_table "spots", id: :integer, charset: "utf8mb4", collation: "utf8mb4_unicode_ci", force: :cascade do |t|
    t.string "place_id", null: false
    t.string "name", limit: 500, null: false
    t.string "category", limit: 100, null: false
    t.text "address"
    t.decimal "latitude", precision: 10, scale: 8
    t.decimal "longitude", precision: 11, scale: 8
    t.decimal "rating", precision: 3, scale: 2
    t.integer "user_ratings_total"
    t.integer "price_level"
    t.string "phone_number", limit: 50
    t.string "website", limit: 1000
    t.json "opening_hours"
    t.json "photos"
    t.json "types"
    t.text "vicinity"
    t.string "plus_code", limit: 50
    t.string "region", limit: 50, default: "chubu"
    t.timestamp "created_at", default: -> { "CURRENT_TIMESTAMP" }
    t.timestamp "updated_at", default: -> { "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" }
    t.json "image_urls", comment: "永続的な画像URLリスト"
    t.string "fallback_image_url", limit: 500, comment: "フォールバック画像URL"
    t.index ["category"], name: "idx_category"
    t.index ["place_id"], name: "idx_place_id"
    t.index ["place_id"], name: "place_id", unique: true
    t.index ["region"], name: "idx_region"
  end

  create_table "user_cards", charset: "utf8mb4", collation: "utf8mb4_0900_ai_ci", force: :cascade do |t|
    t.bigint "user_id", null: false
    t.bigint "card_id", null: false
    t.integer "status", null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["card_id"], name: "index_user_cards_on_card_id"
    t.index ["user_id", "card_id"], name: "index_user_cards_on_user_id_and_card_id", unique: true
    t.index ["user_id"], name: "index_user_cards_on_user_id"
  end

  create_table "users", charset: "utf8mb4", collation: "utf8mb4_0900_ai_ci", force: :cascade do |t|
    t.string "google_id", null: false
    t.string "region", null: false
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["google_id"], name: "index_users_on_google_id", unique: true
  end

  add_foreign_key "review_comments", "cards"
  add_foreign_key "user_cards", "cards"
  add_foreign_key "user_cards", "users"
end
