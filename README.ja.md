# Angelic Angel: 常時収集向け整備ブランチ

Twitter/X の Web Push 通知を Mozilla AutoPush 経由で受信し、明示した種別だけを
耐障害キューへ保存して HTTPS Webhook へ届ける Rust CLI です。

**Draft・実行未検証です。** まだ本番接続や常駐化を行う段階ではありません。
[検証チェックリスト](docs/VERIFICATION.md) と [運用手順](docs/OPERATIONS.ja.md) を確認してください。

主な変更:
- ディスクへの永続化完了後に ACK
- 非同期配送、timeout、backoff、Retry-After、DLQ、再起動をまたぐ重複防止
- 通知種別を明示 allowlist に限定
- Cookie・鍵・本文・URL の出力防止、保護された設定ファイルの原子的保存
- 自動の新規Push登録を停止し、失効時は明示操作を要求
- SIGINT/SIGTERMでの停止、キュー集計、合成データの試験定義

Cookie のコマンド引数指定は廃止しています。init の非表示入力か承認済みの
secret manager を使用し、チャット、GitHub、ログへ秘密情報を入れないでください。

このアプリは送信元IPの固定・匿名化を保証しません。承認済みの独立した実行基盤と
遮断可能な出口制御が別途必要です。収集は通知に限られ、全投稿・全文の保証はありません。

[英語README・CLI例](README.md) / [運用手順](docs/OPERATIONS.ja.md)

MIT。元リポジトリ: [sh1ma/Angelic-Angel](https://github.com/sh1ma/Angelic-Angel)
