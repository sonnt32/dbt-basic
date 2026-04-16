# Nếu bạn có một danh sách 50 mã lỗi Game và ý nghĩa của chúng trong file Excel. Bạn chỉ cần lưu thành .csv, bỏ vào folder seeds, chạy lệnh dbt seed. dbt sẽ tự tạo một bảng trên BigQuery cho bạn.

# Tại sao cấu trúc này lại quan trọng khi triển khai thực tế?
1/ Tính kế thừa: Nếu bạn nghỉ việc, người mới nhìn vào folder staging sẽ biết ngay dữ liệu thô lấy từ đâu.

2/ Dễ bảo trì: Nếu logic Unnest bị sai, bạn chỉ cần sửa ở 1 file trong staging, tất cả các bảng ở tầng intermediate và marts sẽ tự động cập nhật theo.

3/ Tối ưu chi phí: Bạn có thể quy định chỉ tầng marts mới lưu thành bảng (table), còn các tầng khác chỉ là view để tiết kiệm bộ nhớ BigQuery.

# Để bắt đầu triển khai,  cần chuẩn bị:

1/ List bảng nguồn (Firebase/GA4).

2/ Quy tắc đặt tên (Ví dụ: tiền tố stg_ cho staging, fct_ cho bảng sự kiện).

3/ Hệ thống phân quyền (Service Account).