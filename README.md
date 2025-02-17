# DBP-BrewerTemplate
Template repositoty for DBP Brewer

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