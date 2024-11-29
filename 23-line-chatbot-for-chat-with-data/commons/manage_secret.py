import os


def load_secrets(file_path):
    with open(file_path, "r") as file:
        for line in file:
            # แยกข้อมูลโดยใช้ `=` และลบช่องว่างหรือ new line
            name, value = line.strip().split(": ", 1)
            os.environ[name] = value  # ตั้งค่า environment variable
