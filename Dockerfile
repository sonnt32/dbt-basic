FROM ghcr.io/dbt-labs/dbt-bigquery:1.7.0

RUN mkdir -p /root/.dbt /root/.google

# Lưu ý tên file JSON của bạn ở đây
COPY ./data/annoying-puzzle.json /root/.google/annoying-puzzle.json
COPY ./profiles.yml /root/.dbt/profiles.yml

WORKDIR /usr/app/dbt_project
COPY . .

ENTRYPOINT ["dbt"]

