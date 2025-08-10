class CreateReviewComments < ActiveRecord::Migration[8.0]
  def change
    create_table :review_comments do |t|
      t.text :comment
      t.references :card, null: false, foreign_key: true

      t.timestamps
    end
  end
end
