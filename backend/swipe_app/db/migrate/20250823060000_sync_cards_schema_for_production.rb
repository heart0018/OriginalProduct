class SyncCardsSchemaForProduction < ActiveRecord::Migration[8.0]
  def change
    # latitude / longitude を追加（存在しない場合）
    unless column_exists?(:cards, :latitude)
      add_column :cards, :latitude, :decimal, precision: 10, scale: 8
    end

    unless column_exists?(:cards, :longitude)
      add_column :cards, :longitude, :decimal, precision: 11, scale: 8
    end

    # place_id を追加（存在しない場合）+ 一意制約
    unless column_exists?(:cards, :place_id)
      add_column :cards, :place_id, :string, limit: 128
    end

    unless index_exists?(:cards, :place_id, unique: true)
      add_index :cards, :place_id, unique: true
    end

    # image_url の長さを1000に拡張（カラムが存在し、制限が小さい場合でも安全に変更）
    if column_exists?(:cards, :image_url)
      change_column :cards, :image_url, :string, limit: 1000
    end
  end
end
