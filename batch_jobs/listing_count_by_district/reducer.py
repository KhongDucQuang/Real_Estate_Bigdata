import sys

current_district = None
current_count = 0
district = None

# Đọc từng dòng từ Standard Input (đã được Hadoop sort theo key)
for line in sys.stdin:
    line = line.strip()

    # Tách key (district) và value (count '1')
    try:
        district, count_str = line.split('\t', 1)
        count = int(count_str)
    except ValueError:
        # Bỏ qua các dòng không đúng định dạng Key<tab>Value hoặc count không phải số
        continue

    # Nếu district này giống district đang xử lý -> tăng count
    if current_district == district:
        current_count += count
    else:
        # Nếu district thay đổi (hoặc là district đầu tiên)
        # In ra kết quả của district trước đó (nếu có)
        if current_district:
            print(f"{current_district}\t{current_count}")

        # Bắt đầu đếm cho district mới
        current_district = district
        current_count = count

# In ra kết quả cho district cuối cùng sau khi vòng lặp kết thúc
if current_district == district:
    print(f"{current_district}\t{current_count}")