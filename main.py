# main.py
import tkinter as tk
import threading
import traceback
import sys
import os

# اضافه کردن مسیر
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from data_loader import DataLoader
from ui_manager import UIManager

class TSEClient3:
    def __init__(self):
        # ایجاد پنجره اصلی
        self.root = tk.Tk()
        
        # راه‌اندازی اجزای برنامه
        self.config = Config()
        self.data_loader = DataLoader(self.config)
        self.ui = UIManager(self.root, self.config, self.data_loader)
        
        # تنظیمات پنجره
        self.setup_window()
        
        # شروع بارگذاری داده
        self.load_data()
    
    def setup_window(self):
        """تنظیمات پنجره اصلی"""
        # تنظیم موقعیت پنجره در مرکز صفحه
        self.root.update_idletasks()
        width = 1200
        height = 800
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')
    
    def load_data(self):
        """بارگذاری داده در رشته جداگانه"""
        def load_in_thread():
            try:
                # بارگذاری داده‌های TSETMC و داده‌های خارجی (دلار و طلا)
                success, message = self.data_loader.fetch_data()
                
                # ارسال نتیجه به UI در رشته اصلی
                self.root.after(0, lambda: self.ui.on_data_loaded(success, message))
                
            except Exception as e:
                error_msg = f"خطای بحرانی در بارگذاری داده: {str(e)}"
                print(error_msg)
                traceback.print_exc()
                
                # ذخیره خطا در فایل
                self.save_error(error_msg)
                
                # نمایش خطا در UI
                self.root.after(0, lambda: self.ui.on_data_loaded(False, error_msg))
        
        # ایجاد و شروع رشته بارگذاری
        thread = threading.Thread(target=load_in_thread, daemon=True)
        thread.start()
    
    def save_error(self, error_message):
        """ذخیره خطا در فایل"""
        try:
            error_dir = "errors"
            if not os.path.exists(error_dir):
                os.makedirs(error_dir)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_file = os.path.join(error_dir, f"error_{timestamp}.log")
            
            with open(error_file, 'w', encoding='utf-8') as f:
                f.write(f"TSEClient 3.4 Error Log\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Error: {error_message}\n")
                f.write(f"\nTraceback:\n")
                f.write(traceback.format_exc())
                
        except Exception as e:
            print(f"خطا در ذخیره فایل خطا: {e}")
    
    def run(self):
        """اجرای برنامه"""
        try:
            self.root.mainloop()
        except Exception as e:
            error_msg = f"خطای بحرانی در اجرای برنامه: {e}"
            print(error_msg)
            traceback.print_exc()
            self.save_error(error_msg)

def main():
    """تابع اصلی"""
    try:
        app = TSEClient3()
        app.run()
        
    except Exception as e:
        error_msg = f"خطای بحرانی: {e}"
        print(error_msg)
        traceback.print_exc()
        
        # ذخیره خطا
        try:
            with open("critical_error.log", "w", encoding="utf-8") as f:
                f.write(f"Critical Error: {e}\n")
                f.write(traceback.format_exc())
        except:
            pass

if __name__ == "__main__":
    # import اضافی برای تابع save_error
    from datetime import datetime
    main()

# --- IDE variable snapshot ---
try:
    import json
    _vars_snapshot = {
        k: v for k, v in globals().items()
        if k not in ("__name__", "__file__", "__package__", "__loader__", "__spec__", "__builtins__")
        and isinstance(v, (int, float, str, list, dict, tuple, bool))
    }
    with open(r"E:/tseclient6.0\vars_snapshot.json", "w", encoding="utf-8") as _f:
        json.dump(_vars_snapshot, _f, ensure_ascii=False, indent=2)
except Exception as _e:
    print("خطا در ذخیره متغیرها:", _e)

# --- IDE variable snapshot ---
try:
    import json
    _vars_snapshot = {
        k: v for k, v in globals().items()
        if k not in ("__name__", "__file__", "__package__", "__loader__", "__spec__", "__builtins__")
        and isinstance(v, (int, float, str, list, dict, tuple, bool))
    }
    with open(r"E:/tseclient6.0\vars_snapshot.json", "w", encoding="utf-8") as _f:
        json.dump(_vars_snapshot, _f, ensure_ascii=False, indent=2)
except Exception as _e:
    print("خطا در ذخیره متغیرها:", _e)
