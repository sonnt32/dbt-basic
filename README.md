# 🎮 Game Data Transformation with dbt & Docker

Dự án này là một hệ thống **Data Pipeline** hiện đại, chuyên xử lý dữ liệu từ Firebase/Google Analytics 4 trên nền tảng BigQuery. Dự án sử dụng **dbt** để quản lý logic transform và **Docker** để đóng gói vận hành.

---

## 🏗️ Kiến trúc dữ liệu (Data Architecture)

Dữ liệu được tổ chức theo các lớp logic để tối ưu hóa hiệu suất và chi phí:

1.  **Source Layer (Raw)**: Dữ liệu thô từ GA4/Firebase trong BigQuery.
2.  **Flatten Layer (`event_flatten_raw`)**: Thực hiện Unnest các cấu trúc JSON phức tạp (event_params, user_properties) thành dạng bảng phẳng.
3.  **Base Layer (`event_base`)**: Chuẩn hóa các cột thông tin quan trọng như thiết bị, vị trí, campaign và xử lý logic A/B Testing.



---

## 🛠️ Công nghệ sử dụng
- **dbt-bigquery**: Công cụ chính để xây dựng logic Transform bằng SQL.
- **Docker**: Đóng gói môi trường chạy, đảm bảo code chạy giống nhau ở mọi nơi.
- **Google BigQuery**: Kho dữ liệu (Data Warehouse) lưu trữ và xử lý.
- **Git/GitHub**: Quản lý phiên bản mã nguồn.

---

## 🚀 Hướng dẫn cài đặt và Vận hành

### 1. Chuẩn bị (Prerequisites)
- Đã cài đặt [Docker Desktop](https://www.docker.com/products/docker-desktop/).
- Có file Service Account JSON (`.json`) từ Google Cloud GCP với quyền BigQuery Admin/Data Editor.

### 2. Cấu hình bảo mật
Vì lý do bảo mật, các file nhạy cảm không được lưu trên Git. Bạn cần tự tạo lại cấu trúc sau ở máy local:
1. Tạo thư mục `data/` và copy file key vào: `data/annoying-puzzle-dbt.json`.
2. Đảm bảo file `profiles.yml` ở thư mục gốc có thông tin kết nối đúng với dự án của bạn.

### 3. Build & Run dự án

**Build Image (Chỉ làm khi thay đổi code SQL hoặc Dockerfile):**
```bash
docker build -t dbt-game-app .
