# libpq14-benchmark
Benchmarking the insert speed of pipeline queries with PostgreSQL14 client library.

https://www.insight-tec.com/tech-blog/20220118_postgresql/index.html のコード

## 環境
Ubuntu20, g++9で確認  
ビルド時に足りないライブラリは適宜インストールすること。(libreadlinve-dev, libssl-devなど)

## 使い方
1. ./setup.sh
2. make
3. ./bench postgres://{user}:{password}@{host}:{port}/{database} {件数(default 1000000)} {テスト名(指定なしの場合はすべて)}

### テスト名
+ simple / 値埋め込みSQLを同期呼び出し
+ prepared / プリペアド文を同期呼び出し
+ pipeline / 値埋め込みSQLをパイプライン呼び出し
+ pipelineprepared / プリペアド文をパイプライン呼び出し
+ bulk / バルクの値埋め込みSQLを同期呼び出し
+ copystdin / COPYコマンド
