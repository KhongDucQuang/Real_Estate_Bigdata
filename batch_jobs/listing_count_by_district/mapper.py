#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mapper.py (Simplified for pre-parsed data)"""

import sys
import json

# Đọc từng dòng từ Standard Input
for line in sys.stdin:
    # Bỏ khoảng trắng thừa và thử parse JSON
    line = line.strip()
    if not line:
        continue

    try:
        record = json.loads(line)
        # Lấy trực tiếp giá trị từ trường "quan_huyen"
        district = record.get("quan_huyen")

        # Kiểm tra xem có lấy được quận/huyện không
        if district:
            # Làm sạch dữ liệu quận/huyện nếu cần (ví dụ: loại bỏ khoảng trắng thừa)
            district_clean = district.strip()
            if district_clean:
                # Ghi ra output dạng: Key<tab>Value
                # Key là tên quận/huyện, Value là 1
                print(f"{district_clean}\t1")

    except json.JSONDecodeError:
        # Bỏ qua dòng không phải JSON hợp lệ
        # sys.stderr.write(f"Lỗi parse JSON: {line}\n")
        continue
    except KeyError:
        # Bỏ qua nếu bản ghi không có trường "quan_huyen"
        # sys.stderr.write(f"Thiếu trường 'quan_huyen': {line}\n")
        continue
    except Exception as e:
        # Bỏ qua các lỗi khác
        # sys.stderr.write(f"Lỗi xử lý dòng: {line}, Error: {e}\n")
        continue