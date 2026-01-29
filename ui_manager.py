# ui_manager.py
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, Menu, scrolledtext
import json
import os
import webbrowser
from datetime import datetime
import threading
import pandas as pd
import sys
import traceback

class UIManager:
    def __init__(self, root, config, data_loader):
        self.root = root
        self.config = config
        self.data_loader = data_loader
        
        # دانلودر - اصلاح import
        try:
            from downloader import Downloader
            self.downloader = Downloader(config, data_loader)
        except ImportError as e:
            self.log_error(f"خطا در بارگذاری دانلودر: {e}")
            messagebox.showerror("خطای بارگذاری", "ماژول دانلودر یافت نشد.")
            raise
        
        # وضعیت برنامه
        self.current_page = 0
        self.is_downloading = False
        self.download_thread = None
        
        # متغیرهای UI
        self.market_vars = {}
        self.industry_vars = {}
        self.symbol_vars = {}
        self.column_vars = {}
                # متغیر برای چک‌باکس تعدیل
        self.adjustment_var = None
        
        # راه‌اندازی UI
        self.setup_ui()
        self.setup_menu()
        
        # نمایش صفحه بارگذاری
        self.show_loading_page()
    
    def setup_ui(self):
        """راه‌اندازی رابط کاربری"""
        self.root.title("TSEClient 3 - دریافت داده‌های بورس")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 700)
        
        # آیکن پنجره
        try:
            self.root.iconbitmap(default='icon.ico')
        except:
            pass
        
        # استایل‌ها
        self.setup_styles()
        
        # فریم اصلی
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # ایجاد صفحات
        self.pages = []
        self.create_pages()
        
        # ناوبری
        self.create_navigation()
    
    def setup_styles(self):
        """تنظیم استایل‌ها"""
        style = ttk.Style()
        
        # استایل‌های سفارشی
        style.configure("Title.TLabel", font=("Tahoma", 14, "bold"), foreground="#2c3e50")
        style.configure("Subtitle.TLabel", font=("Tahoma", 11), foreground="#34495e")
        style.configure("Bold.TCheckbutton", font=("Tahoma", 10, "bold"))
        style.configure("Highlight.TFrame", background="#ecf0f1")
        style.configure("Success.TLabel", foreground="#27ae60")
        style.configure("Error.TLabel", foreground="#e74c3c")
        style.configure("Warning.TLabel", foreground="#f39c12")
        
        # استایل برای دکمه‌های اصلی
        style.configure("Primary.TButton", font=("Tahoma", 10, "bold"))
        
        # رنگ‌بندی برای حالت‌های مختلف
        self.colors = {
            'primary': '#3498db',
            'success': '#27ae60',
            'warning': '#f39c12',
            'danger': '#e74c3c',
            'dark': '#2c3e50',
            'light': '#ecf0f1'
        }
    
    def setup_menu(self):
        """ایجاد منو"""
        menubar = Menu(self.root)
        self.root.config(menu=menubar)
        
        # منوی فایل
        file_menu = Menu(menubar, tearoff=0)
        menubar.add_cascade(label="فایل", menu=file_menu)
        file_menu.add_command(label="بارگذاری مجدد داده", 
                            command=self.reload_data,
                            accelerator="Ctrl+R")
        file_menu.add_separator()
        file_menu.add_command(label="خروج", 
                            command=self.root.quit,
                            accelerator="Alt+F4")
        
        # منوی تنظیمات
        settings_menu = Menu(menubar, tearoff=0)
        menubar.add_cascade(label="تنظیمات", menu=settings_menu)
        settings_menu.add_command(label="تنظیمات آدرس‌ها", 
                                command=self.show_settings)
        settings_menu.add_command(label="بازنشانی تنظیمات", 
                                command=self.reset_settings)
        settings_menu.add_separator()
        settings_menu.add_command(label="تنظیمات پیشرفته", 
                                command=self.show_advanced_settings)
        
        # منوی ابزارها
        tools_menu = Menu(menubar, tearoff=0)
        menubar.add_cascade(label="ابزارها", menu=tools_menu)
        tools_menu.add_command(label="بررسی اتصال به اینترنت", 
                             command=self.check_internet_connection)
        tools_menu.add_command(label="بررسی APIها", 
                             command=self.check_apis)
        tools_menu.add_separator()
        tools_menu.add_command(label="پاک کردن حافظه کش", 
                             command=self.clear_cache)
        
        # منوی لاگ
        log_menu = Menu(menubar, tearoff=0)
        menubar.add_cascade(label="لاگ", menu=log_menu)
        log_menu.add_command(label="مشاهده لاگ", 
                           command=self.show_log,
                           accelerator="Ctrl+L")
        log_menu.add_command(label="کپی لاگ", 
                           command=self.copy_log,
                           accelerator="Ctrl+C")
        log_menu.add_command(label="پاک کردن لاگ", 
                           command=self.clear_log)
        log_menu.add_separator()
        log_menu.add_command(label="لاگ سطح بالا", 
                           command=self.enable_debug_log)
        
        # منوی راهنما
        help_menu = Menu(menubar, tearoff=0)
        menubar.add_cascade(label="راهنما", menu=help_menu)
        help_menu.add_command(label="راهنمای استفاده", 
                            command=self.show_help,
                            accelerator="F1")
        help_menu.add_command(label="مستندات API", 
                            command=self.show_api_docs)
        help_menu.add_separator()
        help_menu.add_command(label="بررسی بروزرسانی", 
                            command=self.check_for_updates)
        help_menu.add_command(label="درباره", 
                            command=self.show_about)
        
        # کلیدهای میانبر
        self.root.bind('<Control-r>', lambda e: self.reload_data())
        self.root.bind('<Control-l>', lambda e: self.show_log())
        self.root.bind('<Control-c>', lambda e: self.copy_log())
        self.root.bind('<F1>', lambda e: self.show_help())
    
    def create_pages(self):
        """ایجاد صفحات"""
        # صفحه 0: بارگذاری
        self.page0 = ttk.Frame(self.main_frame)
        self.create_page0()
        self.pages.append(self.page0)
        
        # صفحه 1: کد بازار
        self.page1 = ttk.Frame(self.main_frame)
        self.create_page1()
        self.pages.append(self.page1)
        
        # صفحه 2: صنعت
        self.page2 = ttk.Frame(self.main_frame)
        self.create_page2()
        self.pages.append(self.page2)
        
        # صفحه 3: نمادها (با چک‌باکس مربعی)
        self.page3 = ttk.Frame(self.main_frame)
        self.create_page3()
        self.pages.append(self.page3)
        
        # صفحه 4: ستون‌ها
        self.page4 = ttk.Frame(self.main_frame)
        self.create_page4()
        self.pages.append(self.page4)
        
        # صفحه 5: دانلود
        self.page5 = ttk.Frame(self.main_frame)
        self.create_page5()
        self.pages.append(self.page5)
        
        # ابتدا همه صفحات را پنهان می‌کنیم
        for page in self.pages:
            page.pack_forget()
       
    def create_page0(self):
        """صفحه بارگذاری"""
        # فریم مرکزی
        center_frame = ttk.Frame(self.page0)
        center_frame.place(relx=0.5, rely=0.5, anchor='center')
        
        # عنوان
        title_label = ttk.Label(center_frame, 
                              text="TSEClient 3.0", 
                              font=("Tahoma", 24, "bold"),
                              foreground=self.colors['primary'])
        title_label.pack(pady=(0, 20))
        
        # زیرعنوان
        subtitle_label = ttk.Label(center_frame, 
                                 text="دریافت و پردازش داده‌های بازار بورس ایران",
                                 font=("Tahoma", 12),
                                 foreground=self.colors['dark'])
        subtitle_label.pack(pady=(0, 40))
        
        # پیام بارگذاری
        self.loading_label = ttk.Label(center_frame, 
                                      text="در حال اتصال به TSETMC...",
                                      font=("Tahoma", 11))
        self.loading_label.pack(pady=(0, 20))
        
        # نوار پیشرفت
        self.progress_bar = ttk.Progressbar(center_frame, 
                                          mode='indeterminate', 
                                          length=400)
        self.progress_bar.pack(pady=(0, 20))
        
        # درصد پیشرفت
        self.progress_percent = ttk.Label(center_frame, 
                                        text="0%",
                                        font=("Tahoma", 10))
        self.progress_percent.pack(pady=(0, 10))
        
        # وضعیت
        self.status_label = ttk.Label(center_frame, 
                                     text="",
                                     font=("Tahoma", 9),
                                     foreground=self.colors['dark'])
        self.status_label.pack(pady=(0, 10))
        
        # اطلاعات بارگذاری خارجی
        self.external_status_label = ttk.Label(center_frame, 
                                              text="",
                                              font=("Tahoma", 9))
        self.external_status_label.pack(pady=(0, 10))
        
        # شروع نوار پیشرفت
        self.progress_bar.start(10)
    
    def create_page1(self):
        """صفحه 1: انتخاب کد بازار"""
        # فریم اصلی
        main_container = ttk.Frame(self.page1)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # عنوان
        title_frame = ttk.Frame(main_container)
        title_frame.pack(fill=tk.X, pady=(0, 20))
        
        title_label = ttk.Label(title_frame, 
                              text="مرحله 1: انتخاب کدهای بازار", 
                              style="Title.TLabel")
        title_label.pack(side=tk.LEFT)
        
        # شماره مرحله
        step_label = ttk.Label(title_frame,
                             text="(1/5)",
                             font=("Tahoma", 10),
                             foreground=self.colors['primary'])
        step_label.pack(side=tk.RIGHT)
        
        # توضیحات
        desc_label = ttk.Label(main_container,
                             text="کدهای بازار مورد نظر را انتخاب کنید (کدهای مهم به صورت پررنگ نمایش داده شده‌اند):",
                             style="Subtitle.TLabel")
        desc_label.pack(fill=tk.X, pady=(0, 15))
        
        # فریم برای چک‌باکس‌ها
        container = ttk.Frame(main_container)
        container.pack(fill=tk.BOTH, expand=True)
        
        # ایجاد اسکرول‌بار
        canvas = tk.Canvas(container, highlightthickness=0)
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw", width=canvas.winfo_reqwidth())
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # قرار دادن ویجت‌ها
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.market_frame = scrollable_frame
        
        # گزینه حذف معاملات بلوکی
        options_frame = ttk.Frame(main_container)
        options_frame.pack(fill=tk.X, pady=(20, 10))
        
        self.remove_block_var = tk.BooleanVar(value=self.config.settings.get("remove_block_trades", True))
        block_check = ttk.Checkbutton(options_frame, 
                                    text="حذف معاملات بلوکی از نتایج",
                                    variable=self.remove_block_var,
                                    command=self.save_settings)
        block_check.pack(anchor=tk.W)
        
        # دکمه‌های عملیاتی
        btn_frame = ttk.Frame(main_container)
        btn_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Button(btn_frame, 
                  text="انتخاب همه", 
                  command=self.select_all_markets).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, 
                  text="لغو همه", 
                  command=self.deselect_all_markets).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, 
                  text="انتخاب پیش‌فرض", 
                  command=self.select_default_markets).pack(side=tk.LEFT, padx=5)
        
        # اطلاعات آماری
        info_frame = ttk.LabelFrame(main_container, text="آمار", padding=10)
        info_frame.pack(fill=tk.X, pady=(20, 0))
        
        self.page1_info = ttk.Label(info_frame, text="در حال بارگذاری...")
        self.page1_info.pack()
    
    def create_page2(self):
        """صفحه 2: انتخاب صنعت"""
        # فریم اصلی
        main_container = ttk.Frame(self.page2)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # عنوان
        title_frame = ttk.Frame(main_container)
        title_frame.pack(fill=tk.X, pady=(0, 20))
        
        title_label = ttk.Label(title_frame, 
                              text="مرحله 2: انتخاب گروه‌های صنعت", 
                              style="Title.TLabel")
        title_label.pack(side=tk.LEFT)
        
        # شماره مرحله
        step_label = ttk.Label(title_frame,
                             text="(2/5)",
                             font=("Tahoma", 10),
                             foreground=self.colors['primary'])
        step_label.pack(side=tk.RIGHT)
        
        # توضیحات
        desc_label = ttk.Label(main_container,
                             text="صنایع مورد نظر برای فیلتر کردن نمادها را انتخاب کنید:",
                             style="Subtitle.TLabel")
        desc_label.pack(fill=tk.X, pady=(0, 15))
        
        # دکمه‌های انتخاب سریع
        quick_select_frame = ttk.Frame(main_container)
        quick_select_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(quick_select_frame, 
                  text="انتخاب همه", 
                  command=self.select_all_industries).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_select_frame, 
                  text="لغو همه", 
                  command=self.deselect_all_industries).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_select_frame, 
                  text="انتخاب صنایع اصلی", 
                  command=self.select_main_industries).pack(side=tk.LEFT, padx=2)
        
        # فریم برای چک‌باکس‌ها
        container = ttk.Frame(main_container)
        container.pack(fill=tk.BOTH, expand=True)
        
        # ایجاد اسکرول‌بار
        canvas = tk.Canvas(container, highlightthickness=0)
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw", width=canvas.winfo_reqwidth())
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # قرار دادن ویجت‌ها
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.industry_frame = scrollable_frame
        
        # جستجوی صنعت
        search_frame = ttk.Frame(main_container)
        search_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Label(search_frame, text="جستجوی صنعت:").pack(side=tk.LEFT, padx=(0, 5))
        self.industry_search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=self.industry_search_var, width=30)
        search_entry.pack(side=tk.LEFT, padx=(0, 5))
        search_entry.bind("<KeyRelease>", lambda e: self.filter_industries())
        
        ttk.Button(search_frame, 
                  text="پاک کردن", 
                  command=self.clear_industry_search).pack(side=tk.LEFT)
        
        # اطلاعات آماری
        info_frame = ttk.LabelFrame(main_container, text="آمار", padding=10)
        info_frame.pack(fill=tk.X, pady=(20, 0))
        
        self.page2_info = ttk.Label(info_frame, text="در حال بارگذاری...")
        self.page2_info.pack()
    
    def create_page3(self):
        """صفحه 3: انتخاب نمادها با چک‌باکس مربعی"""
        # فریم اصلی
        main_container = ttk.Frame(self.page3)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # عنوان
        title_frame = ttk.Frame(main_container)
        title_frame.pack(fill=tk.X, pady=(0, 20))
        
        title_label = ttk.Label(title_frame, 
                              text="مرحله 3: انتخاب نمادها", 
                              style="Title.TLabel")
        title_label.pack(side=tk.LEFT)
        
        # شماره مرحله
        step_label = ttk.Label(title_frame,
                             text="(3/5)",
                             font=("Tahoma", 10),
                             foreground=self.colors['primary'])
        step_label.pack(side=tk.RIGHT)
        
        # نوار جستجو
        search_frame = ttk.LabelFrame(main_container, text="جستجوی نمادها", padding=10)
        search_frame.pack(fill=tk.X, pady=(0, 15))
        
        # ردیف اول جستجو
        search_row1 = ttk.Frame(search_frame)
        search_row1.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Label(search_row1, text="جستجو در:").pack(side=tk.LEFT, padx=(0, 5))
        self.search_in_var = tk.StringVar(value="نماد")
        search_in_combo = ttk.Combobox(search_row1, 
                                      textvariable=self.search_in_var,
                                      values=["نماد", "نام شرکت", "هر دو"],
                                      state="readonly",
                                      width=12)
        search_in_combo.pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Label(search_row1, text="نوع جستجو:").pack(side=tk.LEFT, padx=(0, 5))
        self.search_type_var = tk.StringVar(value="شامل")
        search_type_combo = ttk.Combobox(search_row1, 
                                        textvariable=self.search_type_var,
                                        values=["شامل", "شروع با", "پایان با", "دقیق"],
                                        state="readonly",
                                        width=12)
        search_type_combo.pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Label(search_row1, text="متن جستجو:").pack(side=tk.LEFT, padx=(0, 5))
        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(search_row1, textvariable=self.search_var, width=30)
        self.search_entry.pack(side=tk.LEFT, padx=(0, 10))
        self.search_entry.bind("<KeyRelease>", lambda e: self.filter_symbols())
        
        # دکمه‌های جستجو
        search_row2 = ttk.Frame(search_frame)
        search_row2.pack(fill=tk.X)
        
        ttk.Button(search_row2, 
                  text="جستجو", 
                  command=self.filter_symbols).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(search_row2, 
                  text="پاک کردن", 
                  command=self.clear_symbol_search).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(search_row2, 
                  text="نمایش همه", 
                  command=self.show_all_symbols).pack(side=tk.LEFT, padx=2)
        
        # دکمه‌های انتخاب سریع
        selection_frame = ttk.Frame(main_container)
        selection_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(selection_frame, 
                  text="انتخاب همه", 
                  command=self.select_all_symbols).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(selection_frame, 
                  text="لغو همه", 
                  command=self.deselect_all_symbols).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(selection_frame, 
                  text="انتخاب معکوس", 
                  command=self.invert_symbol_selection).pack(side=tk.LEFT, padx=2)
        
        ttk.Button(selection_frame, 
                  text="انتخاب تصادفی (10)", 
                  command=self.select_random_symbols).pack(side=tk.LEFT, padx=2)
        
        # فریم برای چک‌باکس‌ها
        list_frame = ttk.LabelFrame(main_container, text="لیست نمادها", padding=5)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # ایجاد اسکرول‌بار برای لیست چک‌باکس‌ها
        canvas_container = ttk.Frame(list_frame)
        canvas_container.pack(fill=tk.BOTH, expand=True)
        
        canvas = tk.Canvas(canvas_container, highlightthickness=0)
        scrollbar = ttk.Scrollbar(canvas_container, orient="vertical", command=canvas.yview)
        
        self.symbol_checkbox_frame = ttk.Frame(canvas)
        
        self.symbol_checkbox_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=self.symbol_checkbox_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # اطلاعات آماری
        info_frame = ttk.LabelFrame(main_container, text="آمار", padding=10)
        info_frame.pack(fill=tk.X)
        
        stats_frame = ttk.Frame(info_frame)
        stats_frame.pack(fill=tk.X)
        
        self.page3_info = ttk.Label(stats_frame, text="در حال بارگذاری...")
        self.page3_info.pack(side=tk.LEFT)
        
        self.selected_count_label = ttk.Label(stats_frame, 
                                            text="",
                                            foreground=self.colors['success'],
                                            font=("Tahoma", 10, "bold"))
        self.selected_count_label.pack(side=tk.RIGHT)
    
    def create_page4(self):
        """صفحه 4: تنظیمات ستون‌ها"""
        # فریم اصلی
        main_container = ttk.Frame(self.page4)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # عنوان
        title_frame = ttk.Frame(main_container)
        title_frame.pack(fill=tk.X, pady=(0, 20))
        
        title_label = ttk.Label(title_frame, 
                              text="مرحله 4: تنظیمات ستون‌های خروجی", 
                              style="Title.TLabel")
        title_label.pack(side=tk.LEFT)
        
        # شماره مرحله
        step_label = ttk.Label(title_frame,
                             text="(4/5)",
                             font=("Tahoma", 10),
                             foreground=self.colors['primary'])
        step_label.pack(side=tk.RIGHT)
        
        # توضیحات
        desc_label = ttk.Label(main_container,
                             text="ستون‌هایی که می‌خواهید در فایل CSV خروجی قرار گیرند را انتخاب کنید:",
                             style="Subtitle.TLabel")
        desc_label.pack(fill=tk.X, pady=(0, 15))
        
        # دکمه‌های انتخاب سریع - اضافه شدن دکمه جدید
        quick_frame = ttk.Frame(main_container)
        quick_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(quick_frame, 
                  text="انتخاب همه", 
                  command=self.select_all_columns).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_frame, 
                  text="لغو همه", 
                  command=self.deselect_all_columns).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_frame, 
                  text="پیش‌فرض", 
                  command=self.default_columns).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_frame, 
                  text="ستون‌های اصلی", 
                  command=self.select_main_columns).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_frame, 
                  text="ستون‌های قیمت", 
                  command=self.select_price_columns).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_frame, 
                  text="ستون‌های حقیقی/حقوقی", 
                  command=self.select_client_columns).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_frame, 
                  text="ستون‌های ارز و طلا", 
                  command=self.select_currency_columns).pack(side=tk.LEFT, padx=2)
        
        # فریم برای چک‌باکس‌ها
        container = ttk.Frame(main_container)
        container.pack(fill=tk.BOTH, expand=True)
        
        # ایجاد اسکرول‌بار
        canvas = tk.Canvas(container, highlightthickness=0)
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw", width=canvas.winfo_reqwidth())
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # قرار دادن ویجت‌ها
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.column_frame = scrollable_frame
        
        # اطلاعات آماری
        info_frame = ttk.LabelFrame(main_container, text="آمار", padding=10)
        info_frame.pack(fill=tk.X, pady=(20, 0))
        
        self.page4_info = ttk.Label(info_frame, text="در حال بارگذاری...")
        self.page4_info.pack()
        
        # پیش‌نمایش
        preview_frame = ttk.LabelFrame(main_container, text="پیش‌نمایش ستون‌ها", padding=10)
        preview_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.preview_label = ttk.Label(preview_frame, 
                                      text="",
                                      font=("Consolas", 9),
                                      wraplength=800)
        self.preview_label.pack()
    
    def create_page5(self):
        """صفحه 5: دانلود"""
        # فریم اصلی
        main_container = ttk.Frame(self.page5)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # عنوان
        title_frame = ttk.Frame(main_container)
        title_frame.pack(fill=tk.X, pady=(0, 20))
        
        title_label = ttk.Label(title_frame, 
                              text="مرحله 5: دانلود فایل‌ها", 
                              style="Title.TLabel")
        title_label.pack(side=tk.LEFT)
        
        # شماره مرحله
        step_label = ttk.Label(title_frame,
                             text="(5/5)",
                             font=("Tahoma", 10),
                             foreground=self.colors['primary'])
        step_label.pack(side=tk.RIGHT)
        
        # تنظیمات خروجی
        output_frame = ttk.LabelFrame(main_container, text="تنظیمات خروجی", padding=10)
        output_frame.pack(fill=tk.X, pady=(0, 15))
        
        # مسیر خروجی
        path_frame = ttk.Frame(output_frame)
        path_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(path_frame, text="پوشه خروجی:").pack(side=tk.LEFT, padx=(0, 5))
        self.output_dir_var = tk.StringVar(value=self.config.settings.get("output_dir", "."))
        dir_entry = ttk.Entry(path_frame, textvariable=self.output_dir_var, width=50)
        dir_entry.pack(side=tk.LEFT, padx=(0, 5), fill=tk.X, expand=True)
        
        ttk.Button(path_frame, 
                  text="انتخاب...", 
                  command=self.select_output_dir).pack(side=tk.LEFT, padx=(0, 5))
        
        ttk.Button(path_frame, 
                  text="باز کردن", 
                  command=self.open_output_dir).pack(side=tk.LEFT)
        
        # گزینه‌های خروجی
        options_frame = ttk.Frame(output_frame)
        options_frame.pack(fill=tk.X)
        
        # ستون اول
        col1 = ttk.Frame(options_frame)
        col1.pack(side=tk.LEFT, fill=tk.Y, expand=True, padx=(0, 10))
        
        self.delete_old_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(col1, 
                       text="حذف فایل‌های قدیمی",
                       variable=self.delete_old_var).pack(anchor=tk.W)
        
        self.compress_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(col1, 
                       text="فشرده‌سازی فایل‌ها (ZIP)",
                       variable=self.compress_var).pack(anchor=tk.W)
        
        # ستون دوم
        col2 = ttk.Frame(options_frame)
        col2.pack(side=tk.LEFT, fill=tk.Y, expand=True)
        
        self.merge_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(col2, 
                       text="ادغام همه فایل‌ها در یک فایل",
                       variable=self.merge_var).pack(anchor=tk.W)
        
        self.add_timestamp_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(col2, 
                       text="افزودن زمان‌مهر به نام فایل",
                       variable=self.add_timestamp_var).pack(anchor=tk.W)
                
        # ✅ ستون سوم برای گزینه تعدیل
        col3 = ttk.Frame(options_frame)
        col3.pack(side=tk.LEFT, fill=tk.Y, expand=True, padx=(0, 10))
        
        # چک‌باکس اعمال تعدیل - با مقدار پیش‌فرض از تنظیمات
        self.adjustment_var = tk.BooleanVar(value=self.config.get_adjustment_status())
        ttk.Checkbutton(col3, 
                       text="اعمال تعدیل بر روی داده‌ها",
                       variable=self.adjustment_var,
                       command=self.save_adjustment_setting).pack(anchor=tk.W)               
        
        # اطلاعات دانلود
        info_frame = ttk.LabelFrame(main_container, text="اطلاعات دانلود", padding=10)
        info_frame.pack(fill=tk.X, pady=(0, 15))
        
        info_text = """• برای هر نماد یک فایل CSV جداگانه ایجاد می‌شود
• داده‌ها از دو منبع مختلف دریافت و ترکیب می‌شوند
• زمان تقریبی دانلود بستگی به تعداد نمادها دارد
• در صورت قطع اتصال، دانلود از آخرین نقطه ادامه می‌یابد"""
        
        info_label = ttk.Label(info_frame, 
                              text=info_text,
                              justify=tk.LEFT)
        info_label.pack()
        
        # پیشرفت دانلود
        progress_frame = ttk.LabelFrame(main_container, text="پیشرفت دانلود", padding=10)
        progress_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # اطلاعات کلی
        self.download_info_label = ttk.Label(progress_frame, text="آماده برای دانلود")
        self.download_info_label.pack(anchor=tk.W, pady=(0, 5))
        
        # نوار پیشرفت کلی
        self.progress_bar = ttk.Progressbar(progress_frame, mode="determinate")
        self.progress_bar.pack(fill=tk.X, pady=(0, 10))
        
        # درصد پیشرفت
        self.progress_percent_label = ttk.Label(progress_frame, text="0%")
        self.progress_percent_label.pack(anchor=tk.W, pady=(0, 10))
        
        # اطلاعات جاری
        self.current_symbol_label = ttk.Label(progress_frame, text="")
        self.current_symbol_label.pack(anchor=tk.W)
        
        # لاگ دانلود
        log_frame = ttk.LabelFrame(progress_frame, text="لاگ دانلود", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        self.log_text = scrolledtext.ScrolledText(log_frame, 
                                                 height=8, 
                                                 font=("Consolas", 9),
                                                 wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # اضافه کردن منوی راست کلیک برای لاگ
        self.add_context_menu(self.log_text)
        
        # دکمه‌های کنترل دانلود
        control_frame = ttk.Frame(main_container)
        control_frame.pack(fill=tk.X, pady=(10, 0))
        
        # چسباندن دکمه‌ها به سمت راست
        ttk.Frame(control_frame).pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        self.stop_btn = ttk.Button(control_frame, 
                                  text="توقف", 
                                  command=self.stop_download,
                                  state=tk.DISABLED,
                                  style="Primary.TButton")
        self.stop_btn.pack(side=tk.RIGHT, padx=5)
        
        # ✅ اینجا دکمه دانلود به جای پایان قرار می‌گیرد (درخواست کاربر)
        self.start_btn = ttk.Button(control_frame, 
                                   text="شروع دانلود", 
                                   command=self.start_download,
                                   style="Primary.TButton")
        self.start_btn.pack(side=tk.RIGHT, padx=5)
        
        ttk.Button(control_frame, 
                  text="پاک کردن لاگ", 
                  command=self.clear_download_log).pack(side=tk.RIGHT, padx=5)
    def save_adjustment_setting(self):
        """ذخیره تنظیمات تعدیل"""
        self.config.set_adjustment_status(self.adjustment_var.get())
        if self.adjustment_var.get():
            self.log_download("✅ حالت تعدیل فعال شد")
        else:
            self.log_download("⚠️ حالت تعدیل غیرفعال شد")
            
    def create_navigation(self):
        """ایجاد ناوبری"""
        nav_frame = ttk.Frame(self.root)
        nav_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        
        # دکمه قبلی
        self.prev_btn = ttk.Button(nav_frame, 
                                  text="← قبلی", 
                                  command=self.prev_page,
                                  state=tk.DISABLED)
        self.prev_btn.pack(side=tk.LEFT, padx=5)
        
        # برچسب صفحه
        self.page_label = ttk.Label(nav_frame, 
                                   text="",
                                   font=("Tahoma", 10, "bold"),
                                   foreground=self.colors['dark'])
        self.page_label.pack(side=tk.LEFT, expand=True)
        
        # ✅ در صفحه 5، دکمه "پایان" نمایش داده می‌شود
        self.next_btn = ttk.Button(nav_frame, 
                                  text="بعدی →", 
                                  command=self.next_page,
                                  state=tk.DISABLED)
        self.next_btn.pack(side=tk.RIGHT, padx=5)
    
    def show_page(self, page_num):
        """نمایش صفحه"""
        self.current_page = page_num
        
        # پنهان کردن همه صفحات
        for page in self.pages:
            page.pack_forget()
        
        # نمایش صفحه جاری
        if 0 <= page_num < len(self.pages):
            self.pages[page_num].pack(fill=tk.BOTH, expand=True)
            
            # بارگذاری داده‌های مورد نیاز هر صفحه
            if page_num == 1:
                self.load_markets()
            elif page_num == 2:
                self.load_industries()
            elif page_num == 3:
                self.load_symbols_list()
            elif page_num == 4:
                self.load_columns()
            elif page_num == 5:
                self.output_dir_var.set(self.config.settings.get("output_dir", "."))
                self.update_download_info()
        
        # به‌روزرسانی ناوبری
        self.update_navigation()
    
    def update_navigation(self):
        """به‌روزرسانی ناوبری"""
        page_titles = {
            0: "در حال بارگذاری...",
            1: "انتخاب کد بازار",
            2: "انتخاب صنعت", 
            3: "انتخاب نمادها",
            4: "تنظیمات ستون‌ها",
            5: "دانلود فایل‌ها"
        }
        
        # تنظیم دکمه قبلی
        if self.current_page > 1:  # از صفحه 2 به بعد فعال
            self.prev_btn.config(state=tk.NORMAL)
        else:
            self.prev_btn.config(state=tk.DISABLED)
        
        # تنظیم دکمه بعدی
        if self.current_page == 0:  # صفحه بارگذاری
            self.next_btn.config(state=tk.DISABLED)
            self.next_btn.config(text="بعدی →")
        elif self.current_page == 5:  # صفحه آخر
            self.next_btn.config(state=tk.NORMAL)
            self.next_btn.config(text="پایان")  # ✅ اینجا دکمه "پایان" است
        else:  # صفحات میانی
            self.next_btn.config(state=tk.NORMAL)
            self.next_btn.config(text="بعدی →")
        
        # برچسب صفحه
        if self.current_page in page_titles:
            self.page_label.config(text=f"{page_titles[self.current_page]} (صفحه {self.current_page + 1} از 6)")
    
    def next_page(self):
        """صفحه بعد"""
        if self.current_page < 5:
            # اعمال فیلترهای صفحه جاری
            success, message = self.apply_current_filters()
            if success:
                self.show_page(self.current_page + 1)
            else:
                messagebox.showwarning("خطا در اعمال فیلتر", message)
        else:  # صفحه 5 - دکمه "پایان"
            if messagebox.askyesno("پایان کار", "آیا می‌خواهید برنامه را ببندید؟"):
                self.root.quit()
    
    def prev_page(self):
        """صفحه قبل"""
        if self.current_page > 0:
            self.show_page(self.current_page - 1)
    
    def show_loading_page(self):
        """نمایش صفحه بارگذاری"""
        self.show_page(0)
    
    def on_data_loaded(self, success, message):
        """وقتی داده بارگذاری شد"""
        self.progress_bar.stop()
        
        if success:
            self.loading_label.config(text="داده‌ها با موفقیت بارگذاری شد!")
            
            # وضعیت داده‌های خارجی
            external_status = []
            
            # بررسی داده دلار
            if self.data_loader.dollar_data is not None:
                dollar_count = len(self.data_loader.dollar_data)
                if dollar_count > 0:
                    last_date = self.data_loader.dollar_data['recDate'].iloc[-1]
                    external_status.append(f"دلار: {dollar_count} روز (تا {last_date})")
                else:
                    external_status.append("دلار: ۰ روز")
            else:
                external_status.append("دلار: ❌")
            
            # بررسی داده طلا
            if self.data_loader.gold_data is not None:
                gold_count = len(self.data_loader.gold_data)
                if gold_count > 0:
                    last_date = self.data_loader.gold_data['recDate'].iloc[-1]
                    external_status.append(f"طلا: {gold_count} روز (تا {last_date})")
                else:
                    external_status.append("طلا: ۰ روز")
            else:
                external_status.append("طلا: ❌")
            
            # نمایش وضعیت
            external_msg = " | ".join(external_status)
            self.external_status_label.config(text=external_msg, foreground=self.colors['success'])
            self.status_label.config(text=f"{message}", foreground=self.colors['success'])
            self.progress_percent.config(text="100%")
            
            # تاخیر قبل از رفتن به صفحه بعد
            self.root.after(1500, lambda: self.show_page(1))
        else:
            self.loading_label.config(text="خطا در بارگذاری داده‌ها!")
            self.status_label.config(text=f"خطا: {message}", foreground=self.colors['danger'])
            self.progress_percent.config(text="خطا!")
            
            # نمایش پیام خطا و رفتن به صفحه بازار
            messagebox.showerror("خطا در بارگذاری", message)
            self.root.after(2000, lambda: self.show_page(1))
    
    def load_markets(self):
        """بارگذاری کدهای بازار"""
        for widget in self.market_frame.winfo_children():
            widget.destroy()
        
        self.market_vars = {}
        markets = self.data_loader.get_market_codes()
        
        if not markets:
            ttk.Label(self.market_frame, 
                     text="هیچ کد بازاری یافت نشد!",
                     foreground=self.colors['danger']).pack(pady=20)
            return
        
        # گروه‌بندی بازارها
        important_markets = []
        other_markets = []
        
        for market in markets:
            if market.get('bold', False):
                important_markets.append(market)
            else:
                other_markets.append(market)
        
        # نمایش بازارهای مهم
        if important_markets:
            ttk.Label(self.market_frame, 
                     text="بازارهای اصلی:",
                     font=("Tahoma", 10, "bold")).pack(anchor=tk.W, pady=(10, 5))
            
            for market in important_markets:
                var = tk.BooleanVar(value=market['default_selected'])
                self.market_vars[market['code']] = var
                
                text = f"{market['code']} - {market['label']} ({market['count']} نماد)"
                cb = ttk.Checkbutton(self.market_frame, 
                                    text=text, 
                                    variable=var,
                                    style="Bold.TCheckbutton")
                cb.pack(anchor=tk.W, padx=20, pady=2)
        
        # نمایش سایر بازارها
        if other_markets:
            ttk.Label(self.market_frame, 
                     text="سایر بازارها:",
                     font=("Tahoma", 10)).pack(anchor=tk.W, pady=(15, 5))
            
            for market in other_markets:
                var = tk.BooleanVar(value=False)
                self.market_vars[market['code']] = var
                
                text = f"{market['code']} - {market['label']} ({market['count']} نماد)"
                cb = ttk.Checkbutton(self.market_frame, 
                                    text=text, 
                                    variable=var)
                cb.pack(anchor=tk.W, padx=20, pady=2)
        
        # به‌روزرسانی آمار
        self.update_market_stats()
    
    def update_market_stats(self):
        """به‌روزرسانی آمار بازارها"""
        total = len(self.market_vars)
        selected = sum(1 for var in self.market_vars.values() if var.get())
        self.page1_info.config(text=f"تعداد بازارها: {total} | انتخاب شده: {selected}")
    
    def select_all_markets(self):
        """انتخاب همه بازارها"""
        for var in self.market_vars.values():
            var.set(True)
        self.update_market_stats()
    
    def deselect_all_markets(self):
        """لغو انتخاب همه بازارها"""
        for var in self.market_vars.values():
            var.set(False)
        self.update_market_stats()
    
    def select_default_markets(self):
        """انتخاب بازارهای پیش‌فرض"""
        default_markets = self.config.settings.get("default_markets", [])
        for code, var in self.market_vars.items():
            var.set(code in default_markets)
        self.update_market_stats()
    
    def load_industries(self):
        """بارگذاری صنایع"""
        for widget in self.industry_frame.winfo_children():
            widget.destroy()
        
        self.industry_vars = {}
        self.industry_items = []
        
        industries = self.data_loader.get_industries()
        
        if not industries:
            ttk.Label(self.industry_frame, 
                     text="هیچ صنعتی یافت نشد!",
                     foreground=self.colors['danger']).pack(pady=20)
            return
        
        # ذخیره صنایع برای فیلتر کردن
        self.all_industries = industries
        
        # نمایش صنایع
        for industry in industries:
            var = tk.BooleanVar(value=True)
            self.industry_vars[industry['code']] = var
            self.industry_items.append({
                'code': industry['code'],
                'name': industry['name'],
                'count': industry['count'],
                'var': var,
                'widget': None
            })
        
        # نمایش صنایع
        self.display_industries()
        
        # به‌روزرسانی آمار
        self.update_industry_stats()
    
    def display_industries(self):
        """نمایش صنایع"""
        for widget in self.industry_frame.winfo_children():
            widget.destroy()
        
        search_term = self.industry_search_var.get().strip().lower()
        
        displayed = 0
        for item in self.industry_items:
            # اعمال فیلتر جستجو
            if search_term:
                if (search_term not in item['name'].lower() and 
                    search_term not in item['code'].lower()):
                    continue
            
            text = f"{item['code']} - {item['name']} ({item['count']} نماد)"
            cb = ttk.Checkbutton(self.industry_frame, 
                                text=text, 
                                variable=item['var'])
            cb.pack(anchor=tk.W, padx=10, pady=2)
            item['widget'] = cb
            displayed += 1
        
        if displayed == 0:
            ttk.Label(self.industry_frame, 
                     text="هیچ صنعتی با این مشخصات یافت نشد!",
                     foreground=self.colors['warning']).pack(pady=20)
    
    def filter_industries(self):
        """فیلتر کردن صنایع بر اساس جستجو"""
        self.display_industries()
        self.update_industry_stats()
    
    def clear_industry_search(self):
        """پاک کردن جستجوی صنعت"""
        self.industry_search_var.set("")
        self.filter_industries()
    
    def update_industry_stats(self):
        """به‌روزرسانی آمار صنایع"""
        total = len(self.industry_items)
        selected = sum(1 for item in self.industry_items if item['var'].get())
        self.page2_info.config(text=f"تعداد صنایع: {total} | انتخاب شده: {selected}")
    
    def select_all_industries(self):
        """انتخاب همه صنایع"""
        for item in self.industry_items:
            item['var'].set(True)
        self.update_industry_stats()
    
    def deselect_all_industries(self):
        """لغو انتخاب همه صنایع"""
        for item in self.industry_items:
            item['var'].set(False)
        self.update_industry_stats()
    
    def select_main_industries(self):
        """انتخاب صنایع اصلی"""
        main_industries = ['43', '44', '38', '34', '27', '68']  # صنایع اصلی
        for item in self.industry_items:
            item['var'].set(item['code'] in main_industries)
        self.update_industry_stats()
    
    def load_symbols_list(self):
        """بارگذاری لیست نمادها با چک‌باکس مربعی"""
        # پاک کردن فریم قبلی
        for widget in self.symbol_checkbox_frame.winfo_children():
            widget.destroy()
        
        self.symbol_vars = {}
        self.symbol_widgets = []
        symbols = self.data_loader.get_symbols()
        
        if not symbols:
            ttk.Label(self.symbol_checkbox_frame, 
                     text="هیچ نمادی یافت نشد!",
                     foreground=self.colors['danger']).pack(pady=20)
            return
        
        # ذخیره نمادها برای فیلتر و مرتب‌سازی
        self.all_symbols = symbols
        
        # نمایش نمادها
        self.display_symbols()
        
        # به‌روزرسانی آمار
        self.update_symbol_stats()

    def display_symbols(self):
        """نمایش نمادها با چک‌باکس مربعی"""
        # پاک کردن فریم قبلی
        for widget in self.symbol_checkbox_frame.winfo_children():
            widget.destroy()
        
        self.symbol_vars = {}
        self.symbol_widgets = []
        
        search_term = self.search_var.get().strip()
        search_type = self.search_type_var.get()
        search_in = self.search_in_var.get()
        
        # فیلتر کردن نمادها
        filtered_symbols = []
        for symbol in self.all_symbols:
            symbol_text = symbol.get('نماد', '')
            company_text = symbol.get('نام_شرکت', '')
            
            # اعمال فیلتر جستجو
            if search_term:
                if search_in == "نماد":
                    search_text = symbol_text
                elif search_in == "نام شرکت":
                    search_text = company_text
                else:  # "هر دو"
                    search_text = f"{symbol_text} {company_text}"
                
                match = False
                if search_type == "شامل":
                    match = search_term in search_text
                elif search_type == "شروع با":
                    match = search_text.startswith(search_term)
                elif search_type == "پایان با":
                    match = search_text.endswith(search_term)
                elif search_type == "دقیق":
                    match = search_text == search_term
                
                if not match:
                    continue
            
            filtered_symbols.append(symbol)
        
        if not filtered_symbols and search_term:
            ttk.Label(self.symbol_checkbox_frame, 
                     text="هیچ نمادی با مشخصات جستجو یافت نشد!",
                     foreground=self.colors['warning']).pack(pady=20)
            return
        
        # نمایش نمادها با چک‌باکس مربعی
        for idx, symbol in enumerate(filtered_symbols, 1):
            frame = ttk.Frame(self.symbol_checkbox_frame)
            frame.pack(fill=tk.X, padx=5, pady=2)
            
            # ایجاد چک‌باکس مربعی
            var = tk.BooleanVar(value=True)
            self.symbol_vars[symbol['نماد']] = var
            
            # تابع callback برای به‌روزرسانی آمار
            def update_stats(var=var, symbol=symbol['نماد']):
                self.update_symbol_stats()
            
            # ✅ استفاده از چک‌باکس استاندارد Tkinter (مربعی)
            cb = tk.Checkbutton(frame, 
                              variable=var,
                              command=update_stats,
                              bg='white',
                              activebackground='white')
            cb.pack(side=tk.LEFT, padx=(0, 10))
            
            # اطلاعات نماد
            info_text = f"{idx}. {symbol['نماد']} - {symbol['نام_شرکت']}"
            label = tk.Label(frame, 
                            text=info_text,
                            font=("Tahoma", 9),
                            bg='white',
                            anchor='w',
                            justify='left')
            label.pack(side=tk.LEFT, fill=tk.X, expand=True)
            
            # اطلاعات اضافی
            market_info = ttk.Label(frame, 
                                  text=f"{symbol.get('کد_بازار', '')} | {symbol.get('نام_صنعت', '')}",
                                  font=("Tahoma", 8),
                                  foreground=self.colors['dark'])
            market_info.pack(side=tk.RIGHT, padx=(10, 0))
            
            self.symbol_widgets.append({
                'frame': frame,
                'var': var,
                'symbol': symbol['نماد'],
                'checkbox': cb,
                'label': label
            })
        
        # به‌روزرسانی آمار
        self.update_symbol_stats()
    
    def filter_symbols(self):
        """فیلتر کردن نمادها بر اساس جستجو"""
        self.display_symbols()

    def clear_symbol_search(self):
        """پاک کردن جستجوی نماد"""
        self.search_var.set("")
        self.display_symbols()

    def show_all_symbols(self):
        """نمایش همه نمادها"""
        self.search_var.set("")
        self.display_symbols()

    def select_all_symbols(self):
        """انتخاب همه نمادها"""
        for widget in self.symbol_widgets:
            widget['var'].set(True)
            widget['checkbox'].select()  # انتخاب چک‌باکس
        self.update_symbol_stats()

    def deselect_all_symbols(self):
        """لغو انتخاب همه نمادها"""
        for widget in self.symbol_widgets:
            widget['var'].set(False)
            widget['checkbox'].deselect()  # لغو انتخاب چک‌باکس
        self.update_symbol_stats()

    def invert_symbol_selection(self):
        """معکوس کردن انتخاب نمادها"""
        for widget in self.symbol_widgets:
            current = widget['var'].get()
            widget['var'].set(not current)
            if not current:
                widget['checkbox'].select()
            else:
                widget['checkbox'].deselect()
        self.update_symbol_stats()

    def select_random_symbols(self):
        """انتخاب تصادفی 10 نماد"""
        import random
        if self.symbol_widgets:
            # انتخاب 10 نماد تصادفی یا کمتر اگر کل نمادها کمتر از 10 باشد
            num_to_select = min(10, len(self.symbol_widgets))
            selected_indices = random.sample(range(len(self.symbol_widgets)), num_to_select)
            
            # ابتدا همه را لغو انتخاب کنیم
            for widget in self.symbol_widgets:
                widget['var'].set(False)
                widget['checkbox'].deselect()
            
            # سپس موارد تصادفی را انتخاب کنیم
            for idx in selected_indices:
                self.symbol_widgets[idx]['var'].set(True)
                self.symbol_widgets[idx]['checkbox'].select()
            
            self.update_symbol_stats()

    def update_symbol_stats(self):
        """به‌روزرسانی آمار نمادها"""
        total = len(self.symbol_widgets)
        selected = sum(1 for widget in self.symbol_widgets if widget['var'].get())
        
        self.page3_info.config(text=f"تعداد نمادها: {total}")
        self.selected_count_label.config(text=f"انتخاب شده: {selected}")
        
        # تغییر رنگ برچسب تعداد انتخاب شده
        if selected == 0:
            self.selected_count_label.config(foreground=self.colors['danger'])
        elif selected == total:
            self.selected_count_label.config(foreground=self.colors['success'])
        else:
            self.selected_count_label.config(foreground=self.colors['warning'])
    
    def load_columns(self):
        """بارگذاری ستون‌ها - اصلاح ترتیب"""
        for widget in self.column_frame.winfo_children():
            widget.destroy()
        
        self.column_vars = {}
        
        # گروه‌بندی ستون‌ها - adjustment_ratio به انتها منتقل شد
        column_groups = {
            "اطلاعات پایه": ["ticker", "recDate"],
            "قیمت‌ها": ["pf", "pl", "pmin", "pmax"],
            "معاملات": ["vol"],
            "حقیقی - خرید": ["buy_I_Volume", "buy_I_Value", "buy_I_Count"],
            "حقوقی - خرید": ["buy_N_Volume", "buy_N_Value", "buy_N_Count"],
            "حقیقی - فروش": ["sell_I_Volume", "sell_I_Value", "sell_I_Count"],
            "حقوقی - فروش": ["sell_N_Volume", "sell_N_Value", "sell_N_Count"],
            "تاریخ": ["insCode", "jalalidate"],
            "نرخ ارز و طلا": ["dollar", "ounces_gold", "thousand_dollar", "one_ounce"],
            "تعدیل": ["adjustment_ratio"]  # ✅ گروه جدید در انتها
        }
        
        # نمایش ستون‌ها بر اساس گروه
        for group_name, columns in column_groups.items():
            ttk.Label(self.column_frame, 
                     text=f"{group_name}:",
                     font=("Tahoma", 10, "bold")).pack(anchor=tk.W, pady=(10, 5))
            
            for col in columns:
                var = tk.BooleanVar(value=True)
                self.column_vars[col] = var
                
                # نام فارسی ستون
                persian_names = {
                    "ticker": "نماد",
                    "recDate": "تاریخ میلادی",
                    "pf": "اولین قیمت",
                    "pl": "آخرین قیمت",
                    "pmin": "کمترین قیمت",
                    "pmax": "بیشترین قیمت",
                    "vol": "حجم معاملات",
                    "buy_I_Volume": "حجم خرید حقیقی",
                    "buy_I_Value": "ارزش خرید حقیقی",
                    "buy_I_Count": "تعداد خرید حقیقی",
                    "buy_N_Volume": "حجم خرید حقوقی",
                    "buy_N_Value": "ارزش خرید حقوقی",
                    "buy_N_Count": "تعداد خرید حقوقی",
                    "sell_I_Volume": "حجم فروش حقیقی",
                    "sell_I_Value": "ارزش فروش حقیقی",
                    "sell_I_Count": "تعداد فروش حقیقی",
                    "sell_N_Volume": "حجم فروش حقوقی",
                    "sell_N_Value": "ارزش فروش حقوقی",
                    "sell_N_Count": "تعداد فروش حقوقی",
                    "insCode": "کد داخلی",
                    "jalalidate": "تاریخ شمسی",
                    "price_date_iso": "تاریخ قیمت",
                    "dollar": "قیمت دلار (ریال)",
                    "ounces_gold": "قیمت انس طلا (دلار)",
                    "thousand_dollar": "1000 دلار بر اساس قیمت نماد",
                    "one_ounce": "1 انس طلا بر اساس قیمت نماد",
                    "adjustment_ratio": "ضریب تعدیل تجمعی"  # ✅ نام فارسی
                }
                
                display_name = f"{col} ({persian_names.get(col, col)})"
                
                def update_preview(var=var, col=col):
                    self.update_column_stats()
                    self.update_column_preview()
                
                cb = ttk.Checkbutton(self.column_frame, 
                                    text=display_name, 
                                    variable=var,
                                    command=update_preview)
                cb.pack(anchor=tk.W, padx=20, pady=2)
        
        self.update_column_stats()
        self.update_column_preview()
    
    def update_column_stats(self):
        """به‌روزرسانی آمار ستون‌ها"""
        total = len(self.column_vars)
        selected = sum(1 for var in self.column_vars.values() if var.get())
        self.page4_info.config(text=f"تعداد ستون‌ها: {total} | انتخاب شده: {selected}")
    
    def update_column_preview(self):
        """به‌روزرسانی پیش‌نمایش ستون‌ها"""
        selected_columns = [col for col, var in self.column_vars.items() if var.get()]
        preview_text = "، ".join(selected_columns)
        
        if len(preview_text) > 100:
            preview_text = preview_text[:100] + "..."
        
        self.preview_label.config(text=preview_text or "(هیچ ستونی انتخاب نشده)")
    
    def select_all_columns(self):
        """انتخاب همه ستون‌ها"""
        for var in self.column_vars.values():
            var.set(True)
        self.update_column_stats()
        self.update_column_preview()
    
    def deselect_all_columns(self):
        """لغو انتخاب همه ستون‌ها"""
        for var in self.column_vars.values():
            var.set(False)
        self.update_column_stats()
        self.update_column_preview()
    
    def default_columns(self):
        """بازنشانی به ستون‌های پیش‌فرض"""
        from config import DEFAULT_OUTPUT_COLUMNS
        for col, var in self.column_vars.items():
            var.set(col in DEFAULT_OUTPUT_COLUMNS)
        self.update_column_stats()
        self.update_column_preview()
    
    def select_main_columns(self):
        """انتخاب ستون‌های اصلی"""
        main_columns = ["ticker", "pf", "pl", "pmin", "pmax", "vol", "recDate", "jalalidate"]
        for col, var in self.column_vars.items():
            var.set(col in main_columns)
        self.update_column_stats()
        self.update_column_preview()
    
    def select_price_columns(self):
        """انتخاب ستون‌های قیمت"""
        price_columns = ["pf", "pl", "pmin", "pmax", "vol"]
        for col, var in self.column_vars.items():
            var.set(col in price_columns)
        self.update_column_stats()
        self.update_column_preview()
    
    def select_client_columns(self):
        """انتخاب ستون‌های حقیقی/حقوقی"""
        client_columns = ["buy_I_Volume", "buy_N_Volume", "sell_I_Volume", "sell_N_Volume"]
        for col, var in self.column_vars.items():
            var.set(col in client_columns)
        self.update_column_stats()
        self.update_column_preview()
    
    def select_currency_columns(self):
        """انتخاب ستون‌های ارز و طلا"""
        currency_columns = ["dollar", "ounces_gold", "thousand_dollar", "one_ounce"]
        for col, var in self.column_vars.items():
            var.set(col in currency_columns)
        self.update_column_stats()
        self.update_column_preview()
    
    def apply_current_filters(self):
        """اعمال فیلترهای صفحه جاری"""
        try:
            if self.current_page == 1:
                selected = [code for code, var in self.market_vars.items() if var.get()]
                if not selected:
                    return False, "لطفاً حداقل یک کد بازار انتخاب کنید"
                
                success, message = self.data_loader.apply_market_filter(
                    selected, self.remove_block_var.get())
                
                if success:
                    self.config.settings["remove_block_trades"] = self.remove_block_var.get()
                    self.config.save_settings()
                
                return success, message
                
            elif self.current_page == 2:
                selected = [code for code, var in self.industry_vars.items() if var.get()]
                if not selected:
                    return False, "لطفاً حداقل یک صنعت انتخاب کنید"
                
                success, message = self.data_loader.apply_industry_filter(selected)
                return success, message
                
            elif self.current_page == 3:
                # دریافت نمادهای انتخاب شده از چک‌باکس‌ها
                selected = []
                for widget in self.symbol_widgets:
                    if widget['var'].get():
                        selected.append(widget['symbol'])
                
                if not selected:
                    return False, "لطفاً حداقل یک نماد انتخاب کنید"
                
                # ذخیره نمادهای انتخاب شده
                self.data_loader.selected_symbols = selected
                return True, f"{len(selected)} نماد انتخاب شد"
                
            elif self.current_page == 4:
                selected_columns = [col for col, var in self.column_vars.items() if var.get()]
                if not selected_columns:
                    return False, "لطفاً حداقل یک ستون انتخاب کنید"
                
                self.config.settings["selected_columns"] = selected_columns
                self.config.save_settings()
                
                return True, f"{len(selected_columns)} ستون انتخاب شد"
            
            return True, ""
        except Exception as e:
            return False, f"خطا در اعمال فیلتر: {str(e)}"
    
    def select_output_dir(self):
        """انتخاب پوشه خروجی"""
        initial_dir = self.output_dir_var.get()
        if not os.path.exists(initial_dir):
            initial_dir = "."
        
        directory = filedialog.askdirectory(
            title="انتخاب پوشه خروجی",
            initialdir=initial_dir
        )
        
        if directory:
            self.output_dir_var.set(directory)
            self.config.settings["output_dir"] = directory
            self.config.save_settings()
            self.update_download_info()
    
    def open_output_dir(self):
        """باز کردن پوشه خروجی"""
        directory = self.output_dir_var.get()
        if os.path.exists(directory):
            try:
                os.startfile(directory)
            except:
                try:
                    webbrowser.open(f"file://{directory}")
                except:
                    self.log_download(f"خطا در باز کردن پوشه: {directory}")
        else:
            self.log_download(f"پوشه یافت نشد: {directory}")
    
    def update_download_info(self):
        """به‌روزرسانی اطلاعات دانلود"""
        if hasattr(self.data_loader, 'selected_symbols'):
            count = len(self.data_loader.selected_symbols)
            self.download_info_label.config(
                text=f"آماده برای دانلود {count} نماد به پوشه: {self.output_dir_var.get()}"
            )
    
    def start_download(self):
        """شروع دانلود"""
        # بررسی پوشه خروجی
        output_dir = self.output_dir_var.get()
        if not output_dir or not os.path.exists(output_dir):
            messagebox.showerror("خطا", "لطفاً ابتدا یک پوشه خروجی معتبر انتخاب کنید.")
            return
        
        # بررسی انتخاب نمادها
        if not hasattr(self.data_loader, 'selected_symbols') or not self.data_loader.selected_symbols:
            messagebox.showerror("خطا", "هیچ نمادی انتخاب نشده است.")
            return
        
        # بررسی انتخاب ستون‌ها
        selected_columns = [col for col, var in self.column_vars.items() if var.get()]
        if not selected_columns:
            messagebox.showerror("خطا", "هیچ ستونی انتخاب نشده است.")
            return
        
        # تایید شروع دانلود
        if not messagebox.askyesno("شروع دانلود", 
                                 f"آیا می‌خواهید دانلود {len(self.data_loader.selected_symbols)} نماد را شروع کنید؟\nوضعیت تعدیل: {'فعال ✅' if self.adjustment_var.get() else 'غیرفعال ⚠️'}"):

           return
        
        # غیرفعال کردن دکمه‌ها
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.prev_btn.config(state=tk.DISABLED)
        self.next_btn.config(state=tk.DISABLED)
        
        # بازنشانی پیشرفت
        self.progress_bar["value"] = 0
        self.progress_percent_label.config(text="0%")
        self.current_symbol_label.config(text="")
        self.log_text.delete(1.0, tk.END)
        
        # شروع دانلود در رشته جداگانه
        self.is_downloading = True
        self.download_thread = threading.Thread(target=self.download_all_symbols, daemon=True)
        self.download_thread.start()
    
    def download_all_symbols(self):
        """دانلود همه نمادهای انتخاب شده"""
        try:
            selected_symbols = self.data_loader.selected_symbols
            selected_columns = [col for col, var in self.column_vars.items() if var.get()]
            output_dir = self.output_dir_var.get()
            total_symbols = len(selected_symbols)
            successful_downloads = 0
            failed_downloads = []
            
            # ایجاد پوشه خروجی اگر وجود ندارد
            os.makedirs(output_dir, exist_ok=True)
            
            # حذف فایل‌های قدیمی اگر انتخاب شده
            if self.delete_old_var.get():
                self.log_download("در حال حذف فایل‌های قدیمی...")
                for file in os.listdir(output_dir):
                    if file.endswith('.csv'):
                        os.remove(os.path.join(output_dir, file))
                self.log_download("فایل‌های قدیمی حذف شدند.")
            
            for i, symbol in enumerate(selected_symbols, 1):
                if not self.is_downloading:
                    self.log_download("دانلود توسط کاربر متوقف شد.")
                    break
                
                # به‌روزرسانی وضعیت جاری
                progress = (i / total_symbols) * 100
                self.root.after(0, self.update_progress, progress, symbol, i, total_symbols)
                
                # دریافت اطلاعات نماد
                symbol_info = self.data_loader.get_symbol_info(symbol)
                
                if symbol_info is None or (isinstance(symbol_info, pd.Series) and symbol_info.empty):
                    self.log_download(f"خطا: اطلاعات نماد {symbol} یافت نشد")
                    failed_downloads.append(symbol)
                    continue
                
                # استخراج کد داخلی
                try:
                    internal_code = str(symbol_info['کد_داخلی']).strip()
                    if not internal_code or internal_code.lower() in ['nan', 'none', '']:
                        self.log_download(f"خطا: کد داخلی برای نماد {symbol} یافت نشد")
                        failed_downloads.append(symbol)
                        continue
                except Exception as e:
                    self.log_download(f"خطا در دریافت کد داخلی {symbol}: {str(e)}")
                    failed_downloads.append(symbol)
                    continue
                
                # دانلود داده نماد
                # دانلود داده نماد با در نظر گرفتن وضعیت تعدیل
                apply_adjustment = self.adjustment_var.get()
                self.log_download(f"در حال دانلود داده برای نماد {symbol} (تعدیل: {'فعال' if apply_adjustment else 'غیرفعال'})...")
                success, data = self.downloader.download_symbol_data(symbol, internal_code, apply_adjustment)
                
                if success and data is not None and not data.empty:
                    # فیلتر کردن ستون‌ها
                    available_columns = [col for col in selected_columns if col in data.columns]
                    if available_columns:
                        data = data[available_columns]
                    else:
                        self.log_download(f"هشدار: هیچ ستون انتخابی برای {symbol} موجود نیست")
                        available_columns = [col for col in data.columns if col in selected_columns]
                        if available_columns:
                            data = data[available_columns]
                    
                    # ذخیره فایل
                    filename = f"{symbol}.csv"
                    if self.add_timestamp_var.get():
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"{symbol}_{timestamp}.csv"
                    
                    filepath = os.path.join(output_dir, filename)
                    
                    try:
                        data.to_csv(filepath, index=False, encoding='utf-8-sig')
                        self.log_download(f"فایل {filename} ذخیره شد ({len(data)} ردیف)")
                        successful_downloads += 1
                    except Exception as e:
                        self.log_download(f"خطا در ذخیره فایل {filename}: {str(e)}")
                        failed_downloads.append(symbol)
                else:
                    error_msg = data if isinstance(data, str) else "داده‌ای دریافت نشد"
                    self.log_download(f"خطا در دانلود داده {symbol}: {error_msg}")
                    failed_downloads.append(symbol)
                
                # تاخیر برای جلوگیری از محدودیت API
                import time
                time.sleep(0.1)
            
            # پایان دانلود
            self.root.after(0, self.download_finished, successful_downloads, failed_downloads)
            
        except Exception as e:
            self.root.after(0, self.download_error, str(e))
    
    def update_progress(self, progress, symbol, current, total):
        """به‌روزرسانی نوار پیشرفت"""
        self.progress_bar["value"] = progress
        self.progress_percent_label.config(text=f"{progress:.1f}%")
        self.current_symbol_label.config(text=f"در حال دانلود: {symbol} ({current}/{total})")
    
    def download_finished(self, successful, failed):
        """پایان دانلود"""
        self.is_downloading = False
        
        # فعال کردن دکمه‌ها
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.prev_btn.config(state=tk.NORMAL)
        self.next_btn.config(state=tk.NORMAL)
        
        # نمایش پیام نتیجه
        message = f"دانلود کامل شد!\nموفق: {successful} نماد"
        if failed:
            message += f"\nناموفق: {len(failed)} نماد"
        
        self.log_download(f"\n{message}")
        
        if failed:
            self.log_download(f"نمادهای ناموفق: {', '.join(failed)}")
        
        # ذخیره فایل‌های دلار و طلا
        output_dir = self.output_dir_var.get()
        currency_success, currency_message = self.downloader.save_currency_files(output_dir)
        
        if currency_success:
            self.log_download(f"✅ فایل‌های دلار و طلا ذخیره شدند")
            message += f"\n{currency_message}"
        else:
            self.log_download(f"⚠️ {currency_message}")
        
        messagebox.showinfo("پایان دانلود", message)
        
        # پیشنهاد باز کردن پوشه
        if successful > 0 and messagebox.askyesno("باز کردن پوشه", "آیا می‌خواهید پوشه خروجی را باز کنید؟"):
            self.open_output_dir()
    
    
    def download_error(self, error):
        """خطا در دانلود"""
        self.is_downloading = False
        
        # فعال کردن دکمه‌ها
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.prev_btn.config(state=tk.NORMAL)
        self.next_btn.config(state=tk.NORMAL)
        
        self.log_download(f"\nخطا در دانلود: {error}")
        messagebox.showerror("خطا در دانلود", f"خطایی رخ داد:\n{error}")
    
    def stop_download(self):
        """توقف دانلود"""
        self.is_downloading = False
        self.log_download("دانلود متوقف شد...")
        
        # فعال کردن دکمه شروع
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
    
    def clear_download_log(self):
        """پاک کردن لاگ دانلود"""
        self.log_text.delete(1.0, tk.END)
        self.log_download("لاگ پاک شد.")
    
    def log_download(self, message):
        """افزودن پیام به لاگ دانلود"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        self.log_text.update()
    
    def add_context_menu(self, widget):
        """افزودن منوی راست کلیک به ویجت"""
        context_menu = tk.Menu(widget, tearoff=0)
        context_menu.add_command(label="کپی", 
                               command=lambda: widget.event_generate('<<Copy>>'))
        context_menu.add_command(label="برش", 
                               command=lambda: widget.event_generate('<<Cut>>'))
        context_menu.add_command(label="چسباندن", 
                               command=lambda: widget.event_generate('<<Paste>>'))
        context_menu.add_separator()
        context_menu.add_command(label="انتخاب همه", 
                               command=lambda: widget.event_generate('<<SelectAll>>'))
        context_menu.add_separator()
        context_menu.add_command(label="پاک کردن", 
                               command=lambda: widget.delete(1.0, tk.END))
        
        def show_context_menu(event):
            try:
                context_menu.tk_popup(event.x_root, event.y_root)
            finally:
                context_menu.grab_release()
        
        widget.bind("<Button-3>", show_context_menu)
        
        # کلیدهای میانبر
        widget.bind("<Control-c>", lambda e: widget.event_generate('<<Copy>>'))
        widget.bind("<Control-x>", lambda e: widget.event_generate('<<Cut>>'))
        widget.bind("<Control-v>", lambda e: widget.event_generate('<<Paste>>'))
        widget.bind("<Control-a>", lambda e: widget.event_generate('<<SelectAll>>'))
    
    def log_error(self, message):
        """لاگ خطا"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_msg = f"[{timestamp}] ERROR: {message}\n"
        print(error_msg)
        
        # ذخیره در فایل خطا
        try:
            with open("error.log", "a", encoding="utf-8") as f:
                f.write(error_msg)
        except:
            pass
    
    # متدهای منو
    def reload_data(self):
        """بارگذاری مجدد داده"""
        if messagebox.askyesno("بارگذاری مجدد", 
                             "آیا می‌خواهید داده‌ها را از سرور TSETMC مجدداً دریافت کنید؟"):
            self.show_loading_page()
            
            def load_in_thread():
                success, message = self.data_loader.fetch_data()
                self.root.after(0, lambda: self.on_data_loaded(success, message))
            
            threading.Thread(target=load_in_thread, daemon=True).start()
    
    def show_settings(self):
        """نمایش تنظیمات"""
        dialog = tk.Toplevel(self.root)
        dialog.title("تنظیمات آدرس‌ها")
        dialog.geometry("800x600")
        dialog.transient(self.root)
        dialog.grab_set()
        
        main_frame = ttk.Frame(dialog, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(main_frame, 
                 text="تنظیمات آدرس‌های API",
                 font=("Tahoma", 12, "bold")).pack(pady=(0, 20))
        
        # آدرس داده اصلی
        ttk.Label(main_frame, text="آدرس داده TSETMC:").pack(anchor=tk.W)
        data_url_text = scrolledtext.ScrolledText(main_frame, height=3, width=80)
        data_url_text.pack(fill=tk.X, pady=(0, 10))
        data_url_text.insert("1.0", self.config.settings.get("data_url", ""))
        self.add_context_menu(data_url_text)
        
        # آدرس داده حقیقی/حقوقی
        ttk.Label(main_frame, text="آدرس داده حقیقی/حقوقی:").pack(anchor=tk.W)
        client_url_text = scrolledtext.ScrolledText(main_frame, height=3, width=80)
        client_url_text.pack(fill=tk.X, pady=(0, 10))
        client_url_text.insert("1.0", self.config.settings.get("client_url", ""))
        self.add_context_menu(client_url_text)
        
        # آدرس داده قیمت
        ttk.Label(main_frame, text="آدرس داده قیمت:").pack(anchor=tk.W)
        price_url_text = scrolledtext.ScrolledText(main_frame, height=3, width=80)
        price_url_text.pack(fill=tk.X, pady=(0, 10))
        price_url_text.insert("1.0", self.config.settings.get("price_url", ""))
        self.add_context_menu(price_url_text)
        
        # آدرس داده دلار
        ttk.Label(main_frame, text="آدرس داده دلار:").pack(anchor=tk.W)
        dollar_url_text = scrolledtext.ScrolledText(main_frame, height=3, width=80)
        dollar_url_text.pack(fill=tk.X, pady=(0, 10))
        dollar_url_text.insert("1.0", self.config.settings.get("dollar_url", ""))
        self.add_context_menu(dollar_url_text)
        
        # آدرس داده طلا
        ttk.Label(main_frame, text="آدرس داده طلا:").pack(anchor=tk.W)
        gold_url_text = scrolledtext.ScrolledText(main_frame, height=3, width=80)
        gold_url_text.pack(fill=tk.X, pady=(0, 20))
        gold_url_text.insert("1.0", self.config.settings.get("gold_url", ""))
        self.add_context_menu(gold_url_text)
        
        # دکمه‌ها
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(pady=(10, 0))
        
        def save_settings():
            self.config.settings["data_url"] = data_url_text.get("1.0", tk.END).strip()
            self.config.settings["client_url"] = client_url_text.get("1.0", tk.END).strip()
            self.config.settings["price_url"] = price_url_text.get("1.0", tk.END).strip()
            self.config.settings["dollar_url"] = dollar_url_text.get("1.0", tk.END).strip()
            self.config.settings["gold_url"] = gold_url_text.get("1.0", tk.END).strip()
            
            if self.config.save_settings():
                messagebox.showinfo("موفق", "تنظیمات با موفقیت ذخیره شد.")
                dialog.destroy()
            else:
                messagebox.showerror("خطا", "خطا در ذخیره تنظیمات.")
        
        ttk.Button(btn_frame, text="ذخیره", command=save_settings).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="لغو", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="بازنشانی", command=lambda: self.reset_dialog_urls(
            data_url_text, client_url_text, price_url_text, dollar_url_text, gold_url_text)).pack(side=tk.LEFT, padx=5)
    
    def reset_dialog_urls(self, data_widget, client_widget, price_widget, dollar_widget, gold_widget):
        """بازنشانی آدرس‌ها در دیالوگ"""
        default_urls = {
            "data_url": "https://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx?h=0&r=0",
            "client_url": "https://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{inscode}",
            "price_url": "https://cdn.tsetmc.com/api/ClosingPrice/GetChartData/{inscode}/D",
            "dollar_url": "https://dashboard-api.tgju.org/v1/tv2/history?symbol=price_dollar_rl&resolution=1D",
            "gold_url": "https://dashboard-api.tgju.org/v1/tv2/history?symbol=ons&resolution=1D"
        }
        
        data_widget.delete(1.0, tk.END)
        data_widget.insert(1.0, default_urls["data_url"])
        
        client_widget.delete(1.0, tk.END)
        client_widget.insert(1.0, default_urls["client_url"])
        
        price_widget.delete(1.0, tk.END)
        price_widget.insert(1.0, default_urls["price_url"])
        
        dollar_widget.delete(1.0, tk.END)
        dollar_widget.insert(1.0, default_urls["dollar_url"])
        
        gold_widget.delete(1.0, tk.END)
        gold_widget.insert(1.0, default_urls["gold_url"])
    
    def reset_settings(self):
        """بازنشانی تنظیمات"""
        if messagebox.askyesno("بازنشانی تنظیمات", 
                             "آیا می‌خواهید همه تنظیمات به حالت اولیه بازگردد؟\nاین عمل غیرقابل بازگشت است."):
            from config import DEFAULT_OUTPUT_COLUMNS
            
            default_settings = {
                "data_url": "https://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx?h=0&r=0",
                "client_url": "https://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{inscode}",
                "price_url": "https://cdn.tsetmc.com/api/ClosingPrice/GetChartData/{inscode}/D",
                "output_dir": ".",
                "remove_block_trades": True,
                "selected_columns": DEFAULT_OUTPUT_COLUMNS,
                "default_markets": ["300", "303", "309", "313", "400", "403", "404"]
            }
            
            self.config.settings = default_settings
            
            if self.config.save_settings():
                messagebox.showinfo("موفق", "تنظیمات با موفقیت بازنشانی شد.")
                # بارگذاری مجدد تنظیمات در UI
                self.remove_block_var.set(True)
                self.output_dir_var.set(".")
            else:
                messagebox.showerror("خطا", "خطا در بازنشانی تنظیمات.")
    
    def show_advanced_settings(self):
        """نمایش تنظیمات پیشرفته"""
        messagebox.showinfo("تنظیمات پیشرفته", "این بخش در حال توسعه است.")
    
    def check_internet_connection(self):
        """بررسی اتصال به اینترنت"""
        import socket
        
        def check():
            try:
                socket.create_connection(("8.8.8.8", 53), timeout=3)
                return True
            except OSError:
                return False
        
        if check():
            messagebox.showinfo("اتصال اینترنت", "اتصال به اینترنت برقرار است.")
        else:
            messagebox.showerror("اتصال اینترنت", "اتصال به اینترنت قطع است!")
    
    def check_apis(self):
        """بررسی APIها"""
        dialog = tk.Toplevel(self.root)
        dialog.title("بررسی وضعیت APIها")
        dialog.geometry("500x400")
        
        main_frame = ttk.Frame(dialog, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(main_frame, 
                 text="بررسی وضعیت APIها",
                 font=("Tahoma", 12, "bold")).pack(pady=(0, 20))
        
        result_text = scrolledtext.ScrolledText(main_frame, height=15, width=60)
        result_text.pack(fill=tk.BOTH, expand=True)
        
        def check_apis_thread():
            import requests
            import time
            
            apis = [
                ("داده اصلی TSETMC", self.config.settings.get("data_url", "")),
                ("داده حقیقی/حقوقی", self.config.settings.get("client_url", "").format(inscode="123456")),
                ("داده قیمت", self.config.settings.get("price_url", "").format(inscode="123456")),
                ("داده دلار", self.config.settings.get("dollar_url", "")),
                ("داده طلا", self.config.settings.get("gold_url", ""))
            ]
            
            results = []
            
            for name, url in apis:
                result_text.insert(tk.END, f"در حال بررسی {name}...\n")
                result_text.see(tk.END)
                dialog.update()
                
                try:
                    start_time = time.time()
                    response = requests.get(url, timeout=10)
                    end_time = time.time()
                    
                    if response.status_code == 200:
                        results.append(f"✓ {name}: فعال (زمان پاسخ: {end_time-start_time:.2f} ثانیه)")
                    else:
                        results.append(f"✗ {name}: خطا کد {response.status_code}")
                
                except Exception as e:
                    results.append(f"✗ {name}: خطا - {str(e)}")
                
                time.sleep(0.5)
            
            # نمایش نتایج
            result_text.delete(1.0, tk.END)
            result_text.insert(1.0, "\n".join(results))
            
            if all("✓" in r for r in results):
                result_text.insert(tk.END, "\n\n✅ همه APIها در دسترس هستند.")
            else:
                result_text.insert(tk.END, "\n\n⚠️ برخی APIها مشکل دارند.")
        
        threading.Thread(target=check_apis_thread, daemon=True).start()
        
        ttk.Button(main_frame, 
                  text="بستن", 
                  command=dialog.destroy).pack(pady=(10, 0))
    
    def clear_cache(self):
        """پاک کردن حافظه کش"""
        if messagebox.askyesno("پاک کردن کش", 
                             "آیا می‌خواهید حافظه کش برنامه پاک شود؟"):
            # پاک کردن داده‌های بارگذاری شده
            self.data_loader.raw_data = None
            self.data_loader.filtered_data = None
            
            # پاک کردن فایل‌های موقت
            temp_files = ["tsetmc_config.json", "error.log"]
            for file in temp_files:
                if os.path.exists(file):
                    try:
                        os.remove(file)
                    except:
                        pass
            
            messagebox.showinfo("موفق", "حافظه کش پاک شد.")
    
    def show_log(self):
        """نمایش لاگ"""
        dialog = tk.Toplevel(self.root)
        dialog.title("مشاهده لاگ برنامه")
        dialog.geometry("900x600")
        
        main_frame = ttk.Frame(dialog, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # ایجاد تب‌های مختلف
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill=tk.BOTH, expand=True)
        
        # تب لاگ برنامه
        app_log_frame = ttk.Frame(notebook)
        notebook.add(app_log_frame, text="لاگ برنامه")
        
        app_log_text = scrolledtext.ScrolledText(app_log_frame, wrap=tk.WORD)
        app_log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # تب لاگ دانلود
        download_log_frame = ttk.Frame(notebook)
        notebook.add(download_log_frame, text="لاگ دانلود")
        
        download_log_text = scrolledtext.ScrolledText(download_log_frame, wrap=tk.WORD)
        download_log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # بارگذاری لاگ‌ها
        def load_logs():
            # لاگ برنامه
            log_content = self.get_log_content()
            app_log_text.insert(1.0, log_content or "فایل لاگی یافت نشد")
            self.add_context_menu(app_log_text)
            
            # لاگ دانلود
            download_log_text.insert(1.0, self.log_text.get(1.0, tk.END))
            self.add_context_menu(download_log_text)
        
        load_logs()
        
        # دکمه‌ها
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(pady=(10, 0))
        
        ttk.Button(btn_frame, 
                  text="بارگذاری مجدد", 
                  command=load_logs).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(btn_frame, 
                  text="کپی", 
                  command=lambda: self.copy_to_clipboard(app_log_text)).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(btn_frame, 
                  text="بستن", 
                  command=dialog.destroy).pack(side=tk.LEFT, padx=5)
    
    def get_log_content(self):
        """دریافت محتوای لاگ"""
        log_dir = "logs"
        if not os.path.exists(log_dir):
            return "پوشه لاگ وجود ندارد"
        
        try:
            log_files = sorted([f for f in os.listdir(log_dir) if f.endswith('.log')], reverse=True)
            if not log_files:
                return "فایل لاگی یافت نشد"
            
            latest_log = os.path.join(log_dir, log_files[0])
            with open(latest_log, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            return f"خطا در خواندن لاگ: {str(e)}"
    
    def copy_log(self):
        """کپی لاگ به کلیپ‌بورد"""
        content = self.get_log_content()
        self.root.clipboard_clear()
        self.root.clipboard_append(content)
        messagebox.showinfo("کپی", "لاگ کپی شد.")
    
    def copy_to_clipboard(self, text_widget):
        """کپی متن به کلیپ‌بورد"""
        try:
            content = text_widget.get(1.0, tk.END)
            self.root.clipboard_clear()
            self.root.clipboard_append(content)
            messagebox.showinfo("کپی", "متن کپی شد.")
        except:
            messagebox.showerror("خطا", "خطا در کپی متن.")
    
    def clear_log(self):
        """پاک کردن لاگ"""
        if messagebox.askyesno("پاک کردن لاگ", 
                             "آیا می‌خواهید همه فایل‌های لاگ پاک شوند؟"):
            log_dir = "logs"
            if os.path.exists(log_dir):
                try:
                    for file in os.listdir(log_dir):
                        if file.endswith('.log'):
                            os.remove(os.path.join(log_dir, file))
                    messagebox.showinfo("موفق", "لاگ‌ها پاک شدند.")
                except Exception as e:
                    messagebox.showerror("خطا", f"خطا در پاک کردن لاگ: {str(e)}")
            else:
                messagebox.showinfo("اطلاع", "پوشه لاگ وجود ندارد.")
    
    def enable_debug_log(self):
        """فعال‌سازی لاگ سطح بالا"""
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
        messagebox.showinfo("لاگ سطح بالا", "لاگ سطح بالا فعال شد.")
    
    def show_help(self):
        """نمایش راهنما"""
        help_text = """📖 راهنمای استفاده از TSEClient 3.0

مرحله 1: انتخاب کدهای بازار
• بازارهای اصلی (بورس، فرابورس، پایه) به صورت پررنگ نمایش داده شده‌اند
• می‌توانید بازارهای مورد نظر را انتخاب کنید
• گزینه "حذف معاملات بلوکی" برای فیلتر کردن معاملات بلوکی است

مرحله 2: انتخاب صنعت
• همه صنایع به طور پیش‌فرض انتخاب شده‌اند
• می‌توانید صنایع مورد نظر را انتخاب یا لغو کنید
• از جستجو برای یافتن صنعت خاص استفاده کنید

مرحله 3: انتخاب نمادها
• لیست نمادهای فیلتر شده نمایش داده می‌شود
• از جستجوی پیشرفته برای فیلتر کردن نمادها استفاده کنید
• می‌توانید نمادها را بر اساس معیارهای مختلف مرتب‌سازی کنید

مرحله 4: تنظیمات ستون‌ها
• ستون‌های مورد نظر برای خروجی CSV را انتخاب کنید
• ستون‌ها به صورت گروه‌بندی شده نمایش داده شده‌اند
• از دکمه‌های انتخاب سریع استفاده کنید

مرحله 5: دانلود فایل‌ها
• پوشه خروجی را انتخاب کنید
• گزینه‌های مختلف دانلود را تنظیم کنید
• دکمه "شروع دانلود" را بزنید

⚠️ نکات مهم:
• اتصال اینترنت پایدار داشته باشید
• فضای کافی در دیسک داشته باشید
• در صورت خطا، لاگ را بررسی کنید

📞 پشتیبانی: در صورت مشکل با توسعه‌دهنده تماس بگیرید."""
        
        self.show_text_dialog("راهنمای استفاده", help_text)
    
    def show_api_docs(self):
        """نمایش مستندات API"""
        docs_text = """📚 مستندات APIهای استفاده شده

1. داده اصلی TSETMC
• آدرس: https://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx?h=0&r=0
• فرمت: متن با جداکننده @ و ;
• اطلاعات: لیست کامل نمادها با اطلاعات بازار

2. داده حقیقی/حقوقی
• آدرس: https://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{inscode}
• فرمت: JSON
• اطلاعات: تاریخچه خرید و فروش حقیقی و حقوقی

3. داده قیمت
• آدرس: https://cdn.tsetmc.com/api/ClosingPrice/GetChartData/{inscode}/D
• فرمت: JSON
• اطلاعات: تاریخچه قیمت‌های روزانه

🔧 پارامترها:
• inscode: کد داخلی نماد (InsCode)
• D: بازه روزانه (Daily)

⚡ محدودیت‌ها:
• نرخ درخواست: حداکثر 10 درخواست در ثانیه
• حجم داده: داده‌های 1 سال گذشته
• فرمت خروجی: UTF-8

📊 ساختار داده:
هر نماد شامل اطلاعات زیر است:
• اطلاعات پایه (نماد، شرکت، صنعت)
• اطلاعات قیمت (باز، بسته، کمترین، بیشترین)
• اطلاعات معاملات (حجم، ارزش)
• اطلاعات حقیقی/حقوقی (حجم، ارزش، تعداد)"""
        
        self.show_text_dialog("مستندات API", docs_text)
    
    def show_text_dialog(self, title, text):
        """نمایش دیالوگ با متن"""
        dialog = tk.Toplevel(self.root)
        dialog.title(title)
        dialog.geometry("700x500")
        
        main_frame = ttk.Frame(dialog, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        text_widget = scrolledtext.ScrolledText(main_frame, wrap=tk.WORD, font=("Tahoma", 10))
        text_widget.pack(fill=tk.BOTH, expand=True)
        text_widget.insert(1.0, text)
        text_widget.config(state=tk.DISABLED)  # فقط خواندنی
        
        self.add_context_menu(text_widget)
        
        ttk.Button(main_frame, 
                  text="بستن", 
                  command=dialog.destroy).pack(pady=(10, 0))
    
    def check_for_updates(self):
        """بررسی بروزرسانی"""
        messagebox.showinfo("بررسی بروزرسانی", 
                          "نسخه فعلی: 3.0\nآخرین بروزرسانی: بهمن ۱۴۰۲\n\nبررسی آنلاین بروزرسانی در دست توسعه است.")
    
    def show_about(self):
        """درباره برنامه"""
        about_text = """TSEClient 3.0

یک برنامه حرفه‌ای برای دریافت و پردازش داده‌های بازار بورس ایران

✨ ویژگی‌ها:
• دریافت داده از TSETMC
• فیلتر پیشرفته بازار و صنعت
• انتخاب هوشمند نمادها
• تنظیمات کامل ستون‌های خروجی
• دانلود گروهی با قابلیت توقف
• لاگ‌گیری پیشرفته
• پشتیبانی از UTF-8

🛠 فناوری‌های استفاده شده:
• Python 3.8+
• Tkinter برای رابط کاربری
• Pandas برای پردازش داده
• Requests برای ارتباط با API

📊 خروجی:
• فایل CSV جداگانه برای هر نماد
• فرمت UTF-8 با BOM
• سازگار با Excel و نرم‌افزارهای تحلیل

👨‍💻 توسعه‌دهنده:
• تیم توسعه TSEClient
• پشتیبانی و به‌روزرسانی مستمر

📄 مجوز:
• نرم‌افزار رایگان و متن‌باز
• قابل استفاده برای اهداف تجاری و غیرتجاری

🔗 منابع:
• داده‌ها از سایت TSETMC دریافت می‌شوند
• کد منبع در GitHub در دسترس است

📞 ارتباط:
• گزارش خطا و پیشنهادات: از طریق Issues"""
        
        self.show_text_dialog("درباره TSEClient 3.0", about_text)
    
    def save_settings(self):
        """ذخیره تنظیمات"""
        self.config.settings["remove_block_trades"] = self.remove_block_var.get()
        self.config.save_settings()

# تابع کمکی برای ایجاد برنامه
def run_ui(config, data_loader):
    """اجرای رابط کاربری"""
    root = tk.Tk()
    ui = UIManager(root, config, data_loader)
    root.mainloop()

if __name__ == "__main__":
    # تست اجرای مستقل
    from config import Config
    from data_loader import DataLoader
    
    config = Config()
    data_loader = DataLoader(config)
    
    root = tk.Tk()
    app = UIManager(root, config, data_loader)
    root.mainloop()