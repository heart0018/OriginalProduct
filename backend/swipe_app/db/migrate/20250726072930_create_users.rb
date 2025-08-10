class CreateUsers < ActiveRecord::Migration[8.0]
  def change
    create_table :users do |t|
      t.string :google_id, null: false
      t.string :region, null: false

      t.timestamps
    end
    add_index :users, :google_id, unique: true
  end
end
