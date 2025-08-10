class RenameTypeToGenreInCards < ActiveRecord::Migration[8.0]
  def change
    rename_column :cards, :type, :genre
  end
end
