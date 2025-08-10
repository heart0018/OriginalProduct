class CreateCards < ActiveRecord::Migration[8.0]
  def change
    create_table :cards do |t|
      t.string  :genre, limit: 32                      # カード種別（type→genreに変更）
      t.string  :title, null: false, limit: 128        # 店名・スポット名
      t.float   :rating, null: false, default: 0.0     # 平均評価
      t.integer :review_count, null: false, default: 0
      t.string  :image_url, limit: 256
      t.string  :external_link, limit: 256
      t.string  :region, limit: 16
      t.string  :address, limit: 128

      t.timestamps
    end
  end
end
