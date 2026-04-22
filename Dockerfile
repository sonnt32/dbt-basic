FROM ghcr.io/dbt-labs/dbt-bigquery:1.7.0

RUN mkdir -p /root/.dbt /root/.google

# Copy toàn bộ folder data chứa tất cả các file JSON vào một chỗ
COPY ./data/ /root/.google/

COPY ./profiles.yml /root/.dbt/profiles.yml

WORKDIR /usr/app/dbt_project
COPY . .

ENTRYPOINT ["dbt"]