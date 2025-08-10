class CreateUserCards < ActiveRecord::Migration[8.0]
  def change
    create_table :user_cards do |t|
      t.references :user, null: false, foreign_key: true
      t.references :card, null: false, foreign_key: true
      t.integer :status, null: false  # ENUM代用

      t.timestamps
    end
    add_index :user_cards, [:user_id, :card_id], unique: true
  end
end
