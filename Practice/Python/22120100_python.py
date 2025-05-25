import pandas as pd

def sum_of_odd_numbers(n):
    if n < 1:
        return 0
    return sum(i for i in range(1, n + 1) if i % 2 != 0)

def numbers_divisible_by_7_not_5(start, end):
    return [i for i in range(start, end + 1) if i % 7 == 0 and i % 5 != 0]

def reverse_string(s):
    return s[::-1]

def factorial(n):
    if n < 0:
        raise ValueError("Không thể tính giai thừa cho số âm.")
    if n == 0 or n == 1:
        return 1
    return n * factorial(n - 1)

def FizzBuzz(n):
    for i in range(1, n + 1):
        if i % 3 == 0 and i % 5 == 0:
            print("FizzBuzz")
        elif i % 3 == 0:
            print("Fizz")
        elif i % 5 == 0:
            print("Buzz")
        else:
            print(i)

def is_palindrome(s):
    return s == s[::-1]

def max_number(nums):
    return max(nums)

def remove_duplicates(lst):
    return list(dict.fromkeys(lst))

def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True

def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError:
        print("File không tồn tại.")
    except pd.errors.EmptyDataError:
        print("File rỗng.")
    except pd.errors.ParserError:
        print("Lỗi phân tích cú pháp.")
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")
    return None

def calculate_sum_or_average(df, column_name):
    if column_name not in df.columns:
        print(f"Cột '{column_name}' không tồn tại trong DataFrame.")
        return None
    total = df[column_name].sum()
    average = df[column_name].mean()
    return total, average

def write_csv_file(file_path, df):
    try:
        df.to_csv(file_path, index=False)
        print("Dữ liệu đã được ghi vào file CSV thành công.")
    except Exception as e:
        print(f"Có lỗi khi ghi file: {e}")

def menu():
    while True:
        print("\n===== MENU =====")
        print("1. Tổng các số lẻ từ 1 đến n")
        print("2. Các số chia hết cho 7 nhưng không chia hết cho 5 (2000–3200)")
        print("3. Đảo ngược chuỗi")
        print("4. Tính giai thừa")
        print("5. FizzBuzz")
        print("6. Kiểm tra chuỗi Palindrome")
        print("7. Tìm số lớn nhất trong danh sách")
        print("8. Xóa phần tử trùng lặp trong danh sách")
        print("9. Kiểm tra số nguyên tố")
        print("10. Tính tổng danh sách số")
        print("11. Đọc dữ liệu từ file CSV")
        print("12. Tính tổng và trung bình của cột trong CSV")
        print("13. Thêm dữ liệu vào file CSV")
        print("0. Thoát")
        choice = input("Chọn chức năng (0-13): ")

        if choice == '1':
            n = int(input("Nhập n: "))
            print("Tổng số lẻ:", sum_of_odd_numbers(n))

        elif choice == '2':
            res = numbers_divisible_by_7_not_5(2000, 3200)
            print(res)

        elif choice == '3':
            s = input("Nhập chuỗi: ")
            print("Kết quả:", reverse_string(s))

        elif choice == '4':
            try:
                n = int(input("Nhập số nguyên dương: "))
                print("Giai thừa:", factorial(n))
            except ValueError as ve:
                print("Lỗi:", ve)

        elif choice == '5':
            n = int(input("FizzBuzz đến số bao nhiêu? "))
            FizzBuzz(n)

        elif choice == '6':
            s = input("Nhập chuỗi: ")
            if is_palindrome(s):
                print(f"'{s}' là chuỗi Palindrome")
            else:
                print(f"'{s}' không phải là chuỗi Palindrome")

        elif choice == '7':
            nums = list(map(int, input("Nhập các số cách nhau bởi dấu cách: ").split()))
            print("Số lớn nhất:", max_number(nums))

        elif choice == '8':
            items = input("Nhập danh sách cách nhau bởi dấu cách: ").split()
            print("Danh sách sau khi xóa trùng:", remove_duplicates(items))

        elif choice == '9':
            n = int(input("Nhập số nguyên dương: "))
            if is_prime(n):
                print(f"{n} là số nguyên tố")
            else:
                print(f"{n} không phải là số nguyên tố")

        elif choice == '10':
            lst = list(map(int, input("Nhập danh sách số: ").split()))
            print("Tổng:", sum(lst))

        elif choice == '11':
            file_path = input("Nhập tên file CSV: ")
            df = read_csv_file(file_path)
            if df is not None:
                print(df.head())

        elif choice == '12':
            file_path = input("Nhập tên file CSV: ")
            column_name = input("Nhập tên cột: ")
            df = read_csv_file(file_path)
            if df is not None:
                result = calculate_sum_or_average(df, column_name)
                if result:
                    total, avg = result
                    print(f"Tổng: {total}, Trung bình: {avg}")

        elif choice == '13':
            file_path = input("Nhập tên file CSV: ")
            new_data = input("Nhập dữ liệu mới (cách nhau bằng dấu phẩy): ").split(",")
            df = read_csv_file(file_path)
            if df is not None:
                if len(new_data) != len(df.columns):
                    print("Số lượng dữ liệu không khớp số cột.")
                    continue
                new_row = pd.DataFrame([new_data], columns=df.columns)
                df = pd.concat([df, new_row], ignore_index=True)
                write_csv_file(file_path, df)

        elif choice == '0':
            print("Thoát chương trình.")
            break

        else:
            print("Lựa chọn không hợp lệ. Vui lòng chọn lại.")

if __name__ == "__main__":
    menu()
