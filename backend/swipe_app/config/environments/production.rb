require "active_support/core_ext/integer/time"

Rails.application.configure do
  # 本番向けの基本設定（ローカル検証中は一部緩め）
  config.enable_reloading = false
  config.eager_load = true

  # 例外詳細は本番では非表示（調査時のみ true に）
  config.consider_all_requests_local = false

  # キャッシュ/静的ファイル
  config.action_controller.perform_caching = true
  config.public_file_server.enabled = true
  config.public_file_server.headers = { "Cache-Control" => "public, max-age=#{1.year.to_i}" }

  # Active Storage
  config.active_storage.service = :local

  # SSL（ローカル検証では false）
  config.assume_ssl = false
  config.force_ssl  = false

  # ログ
  config.log_tags = [ :request_id ]
  config.logger   = ActiveSupport::TaggedLogging.logger($stdout)
  config.log_level = (ENV["RAILS_LOG_LEVEL"] || "info")
  config.silence_healthcheck_path = "/up"
  config.active_support.report_deprecations = false

  # I18n/AR
  config.i18n.fallbacks = true
  config.active_record.dump_schema_after_migration = false
  config.active_record.attributes_for_inspect = [ :id ]
end
