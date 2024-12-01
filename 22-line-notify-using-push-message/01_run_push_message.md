# LINE Notification

## Instruction

1. เปลี่ยนไปยังโฟลเดอร์โปรเจกต์
   ใช้คำสั่ง cd เพื่อเปลี่ยนไปยังโฟลเดอร์ชื่อ 22-line-notify-using-push-message/

```
cd 22-line-notify-using-push-message/
```

2. คัดลอกไฟล์ตัวอย่างการตั้งค่า
   คำสั่ง cp ใช้เพื่อคัดลอกไฟล์ line_secret.yml.example และเปลี่ยนชื่อเป็น line_secret.yml เพื่อใช้เป็นไฟล์ตั้งค่าหลัก

```bash
cp line_secret.yml.example line_secret.yml
```

3. ติดตั้ง Dependencies ของโปรเจกต์
   ใช้คำสั่ง pip เพื่อติดตั้งไลบรารีทั้งหมดที่ระบุไว้ในไฟล์ requirements.txt

```bash
pip install -r requirements.txt
```

4. รันโปรแกรมส่งข้อความ LINE Notify
   ใช้คำสั่ง python เพื่อรันไฟล์ push_message.py ซึ่งเป็นสคริปต์หลักของโปรเจกต์
   `python push_message.py`

5. ลองรัน `python push_coingecko_flex_message.py` จะมี Flex Message ส่งมา
