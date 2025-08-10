# config/routes.rb
# APIのルーティング設定
# config/routes.rb
Rails.application.routes.draw do
  get "up" => "rails/health#show", as: :rails_health_check

  namespace :api do
    namespace :v1 do
      resources :cards, only: [ :index, :show ]
    end
  end
end


# 保存リスト機能実装時にpostメソッドの追加
