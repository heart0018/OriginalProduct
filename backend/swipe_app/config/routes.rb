# config/routes.rb
# APIのルーティング設定
# config/routes.rb
Rails.application.routes.draw do
  # Health check
  get "up" => "rails/health#show", as: :rails_health_check

  namespace :api do
    namespace :v1 do
      # Google OAuth
      post "auth/google", to: "auth#google"

      # セッション（単一リソース）
      resource :session, only: [ :show, :destroy ]

      # カード
      resources :cards, only: [ :index, :show ]
    end
  end
end


# 保存リスト機能実装時にpostメソッドの追加
