# VSCode ใน GitHub Codespace

## สารบัญ
1. [ส่วนประกอบหลักของ VSCode](#สวนประกอบหลกของ-vscode)
2. [การใช้งานพื้นฐาน](#การใชงานพนฐาน)
3. [Extensions ที่แนะนำ](#extensions-ทแนะนำ)
4. [Shortcuts ที่สำคัญ](#shortcuts-ทสำคญ)
5. [การตั้งค่าที่ควรรู้](#การตงคาทควรร)

## ส่วนประกอบหลักของ VSCode

### 1. Activity Bar
- 📁 Explorer (Ctrl+Shift+E)
- 🔍 Search (Ctrl+Shift+F)
- 🔄 Source Control (Ctrl+Shift+G)
- 🐞 Run and Debug (Ctrl+Shift+D)
- 🧩 Extensions (Ctrl+Shift+X)

### 2. Side Bar
- แสดงรายละเอียดของแต่ละ Activity
- ปรับขนาดได้
- ซ่อน/แสดงได้ด้วย Ctrl+B

### 3. Editor Area
- พื้นที่แก้ไขโค้ด
- รองรับการเปิดหลายไฟล์พร้อมกัน
- แบ่งหน้าจอได้ (Split Editor)

### 4. Panel
- Terminal (Ctrl+`)
- Problems
- Output
- Debug Console

### 5. Status Bar
- แสดงข้อมูลสถานะต่างๆ
- Git branch
- File encoding
- Line/Column number

## การใช้งานพื้นฐาน

### การจัดการไฟล์
```plaintext
- New File: Ctrl+N
- Open File: Ctrl+O
- Save: Ctrl+S
- Save All: Ctrl+K S
- Close File: Ctrl+W
```

### การแก้ไขโค้ด
```plaintext
- Copy: Ctrl+C
- Cut: Ctrl+X
- Paste: Ctrl+V
- Undo: Ctrl+Z
- Redo: Ctrl+Y
- Multi-cursor: Alt+Click
- Select All Occurrences: Ctrl+Shift+L
```

## Extensions ที่แนะนำ

### 1. การพัฒนาทั่วไป
- GitHub Copilot
- GitLens
- Docker
- Remote Development

### 2. ภาษาเฉพาะทาง
- Python
- JavaScript and TypeScript
- C/C++
- Java Extension Pack

### 3. เครื่องมือเสริม
- Prettier
- ESLint
- Live Share
- Code Spell Checker

## Shortcuts ที่สำคัญ

### การนำทาง
```plaintext
Ctrl+P          : Quick Open File
Ctrl+Shift+P    : Command Palette
Ctrl+\          : Split Editor
Ctrl+Tab        : Switch between files
```

### การแก้ไขโค้ด
```plaintext
Alt+↑/↓         : Move line up/down
Ctrl+/          : Toggle comment
Ctrl+Space      : Trigger suggestion
F12             : Go to Definition
```

## การตั้งค่าที่ควรรู้

### 1. settings.json
```json
{
    "editor.fontSize": 14,
    "editor.tabSize": 2,
    "editor.wordWrap": "on",
    "editor.formatOnSave": true,
    "files.autoSave": "afterDelay"
}
```

### 2. การตั้งค่า Terminal
```json
{
    "terminal.integrated.fontSize": 14,
    "terminal.integrated.shell.windows": "C:\\Program Files\\Git\\bin\\bash.exe"
}
```

## เทคนิคการใช้งาน

### 1. Zen Mode
- เข้าสู่โหมดเต็มจอ: Ctrl+K Z
- ไม่แสดง UI elements ที่ไม่จำเป็น
- เหมาะสำหรับการโฟกัสกับโค้ด

### 2. Multi-cursor Editing
```plaintext
- Alt+Click: เพิ่ม cursor
- Ctrl+Alt+↑/↓: เพิ่ม cursor ด้านบน/ล่าง
- Ctrl+D: เลือกคำที่เหมือนกันถัดไป
```

### 3. Snippets
```json
{
    "Print to console": {
        "prefix": "log",
        "body": [
            "console.log('$1');",
            "$2"
        ],
        "description": "Log output to console"
    }
}
```

## การแก้ปัญหาที่พบบ่อย

### 1. Performance Issues
- ลด Extensions ที่ไม่จำเป็น
- ล้าง Cache
- ปิดการใช้งาน Telemetry

### 2. Git Integration
- ตรวจสอบ Git credentials
- ใช้ Source Control panel
- รู้จักใช้ GitLens

## แหล่งข้อมูลเพิ่มเติม
- [VSCode Documentation](https://code.visualstudio.com/docs)
- [GitHub Codespaces Documentation](https://docs.github.com/en/codespaces)
- [VSCode Tips and Tricks](https://code.visualstudio.com/docs/getstarted/tips-and-tricks)