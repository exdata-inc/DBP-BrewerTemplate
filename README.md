# DBP-BrewerTemplate

このリポジトリは，醸造プログラムのテンプレートを提供します．
JSON-LD 形式の醸造需要データを受け取り，指定されたデータソースからデータを読み込み，醸造処理を行わずに指定された保存先に保存します．

# Build

```bash
docker build --no-cache -t dbp-brewer-template .
```

## Run

```bash
docker run -it \
  -v <your_mount_directory>:/app/<your_data_directory> \
  dbp-brewer-template \
  "<brewing_demand_json_ld>"
```
