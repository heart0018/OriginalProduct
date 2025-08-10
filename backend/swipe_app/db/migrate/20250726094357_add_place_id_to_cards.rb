class AddPlaceIdToCards < ActiveRecord::Migration[8.0]
  def change
    add_column :cards, :place_id, :string, limit: 128
    add_index :cards, :place_id, unique: true
  end
end
