# config.py
import json
import os
import logging
from datetime import datetime

# نگاشت صنایع
INDUSTRY_MAP = {
    '01': 'زراعت و خدمات وابسته',
    '02': 'جنگلداري و ماهيگيري',
    '10': 'استخراج زغال سنگ',
    '11': 'استخراج نفت گاز و خدمات جنبي جز اکتشاف',
    '13': 'استخراج کانه هاي فلزي',
    '14': 'استخراج ساير معادن',
    '15': 'حذف شده- فرآورده‌هاي غذايي و آشاميدني',
    '17': 'منسوجات',
    '19': 'دباغي، پرداخت چرم و ساخت انواع پاپوش',
    '20': 'محصولات چوبي',
    '21': 'محصولات كاغذي',
    '22': 'انتشار، چاپ و تکثير',
    '23': 'فراورده هاي نفتي، كک و سوخت هسته اي',
    '24': 'حذف شده-مواد و محصولات شيميايي',
    '25': 'لاستيك و پلاستيك',
    '26': 'توليد محصولات كامپيوتري الكترونيكي ونوري',
    '27': 'فلزات اساسي',
    '28': 'ساخت محصولات فلزي',
    '29': 'ماشين آلات و تجهيزات',
    '31': 'ماشين آلات و دستگاه‌هاي برقي',
    '32': 'ساخت دستگاه‌ها و وسايل ارتباطي',
    '33': 'ابزارپزشکي، اپتيکي و اندازه‌گيري',
    '34': 'خودرو و ساخت قطعات',
    '35': 'ساير تجهيزات حمل و نقل',
    '36': 'مبلمان و مصنوعات ديگر',
    '38': 'قند و شكر',
    '39': 'شرکتهاي چند رشته اي صنعتي',
    '40': 'عرضه برق، گاز، بخاروآب گرم',
    '41': 'جمع آوري، تصفيه و توزيع آب',
    '42': 'محصولات غذايي و آشاميدني به جز قند و شكر',
    '43': 'مواد و محصولات دارويي',
    '44': 'محصولات شيميايي',
    '45': 'پيمانكاري صنعتي',
    '46': 'تجارت عمده فروشي به جز وسايل نقليه موتور',
    '47': 'خرده فروشي،باستثناي وسايل نقليه موتوري',
    '49': 'كاشي و سراميك',
    '50': 'تجارت عمده وخرده فروشي وسائط نقليه موتور',
    '51': 'حمل و نقل هوايي',
    '52': 'انبارداري و حمايت از فعاليتهاي حمل و نقل',
    '53': 'سيمان، آهك و گچ',
    '54': 'ساير محصولات كاني غيرفلزي',
    '55': 'هتل و رستوران',
    '56': 'سرمايه گذاريها',
    '57': 'بانكها و موسسات اعتباري',
    '58': 'ساير واسطه گريهاي مالي',
    '59': 'اوراق حق تقدم استفاده از تسهيلات مسكن',
    '60': 'حمل ونقل، انبارداري و ارتباطات',
    '61': 'حمل و نقل آبی',
    '63': 'فعالیت های پشتیبانی و کمکی حمل و نقل',
    '64': 'مخابرات',
    '65': 'واسطه‌گری‌های مالی و پولی',
    '66': 'بیمه وصندوق بازنشستگی به جز تامین اجتماعی',
    '67': 'فعالیت‌هاي کمکی به نهادهای مالی واسط',
    '68': 'صندوق سرمایه گذاری قابل معامله',
    '69': 'اوراق تامین مالی',
    '70': 'انبوه سازی، املاک و مستغلات',
    '71': 'فعالیت مهندسی، تجزیه، تحلیل و آزمایش فنی',
    '72': 'رایانه و فعالیت‌های وابسته به آن',
    '73': 'اطلاعات و ارتباطات',
    '74': 'خدمات فنی و مهندسی',
    '76': 'اوراق بهادار مبتنی بر دارایی فکری',
    '77': 'فعالبت های اجاره و لیزینگ',
    '80': 'تبلیغات و بازارپژوهی',
    '82': 'فعالیت پشتیبانی اجرائی اداری و حمایت کسب',
    '84': 'سلامت انسان و مددکاری اجتماعی',
    '90': 'فعالیت های هنری، سرگرمی و خلاقانه',
    '93': 'فعالیت‌های فرهنگی و ورزشی',
    '98': 'گروه اوراق غیر فعال',
    'X1': 'شاخص'
}

# نگاشت بازارها
MARKET_LABELS = {
    '300': 'بورس',
    '303': 'فرابورس',
    '309': 'پایه',
    '301': 'مشارکت',
    '304': 'آتی',
    '305': 'صندوق',
    '306': 'مرابحه و اجاره',
    '307': 'تسهیلات مسکن',
    '308': 'سلف',
    '311': 'اختیار خ ض',
    '312': 'اختیار ف ط',
    '313': 'بازار نوآفرین رشد پایه',
    '315': 'صندوق کالا',
    '320': 'اختیار خرید ض',
    '321': 'اختیار ف ط',
    '380': 'صندوق طلا و کالا',
    '400': 'حق بورس',
    '403': 'حق فرابورس',
    '404': 'حق پایه',
    '701': 'زعفران و سکه',
    '706': 'مرابحه دولت اراد',
    '803': 'بار برق',
    '804': 'بار برق',
    '200': 'سلف انرژی',
    '206': 'صکوک',
    '201': 'گواهی',
    '208': 'صکوک'
}

# ستون‌های خروجی پیش‌فرض (اضافه شدن ستون adjustment_ratio)
DEFAULT_OUTPUT_COLUMNS = [
    "ticker",
    "pf",
    "pl",
    "pmin",
    "pmax",
    "vol",
    "recDate",
    "jalalidate",
    "buy_I_Volume",
    "buy_I_Value",
    "buy_I_Count",
    "buy_N_Volume",
    "buy_N_Value",
    "buy_N_Count",
    "sell_I_Volume",
    "sell_I_Value",
    "sell_I_Count",
    "sell_N_Volume",
    "sell_N_Value",
    "sell_N_Count",
    "price_date_iso",
    "insCode",
    "dollar",
    "ounces_gold",
    "thousand_dollar",
    "one_ounce",
    "adjustment_ratio"  # ✅ در انتهای لیست
]

class Config:
    def __init__(self):
        self.settings_file = "tsetmc_config.json"
        self.settings = self.load_settings()
        
        # راه‌اندازی لاگ
        self.setup_logging()
    
    def setup_logging(self):
        """راه‌اندازی سیستم لاگ"""
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        log_file = os.path.join(log_dir, f"tsetmc_{datetime.now().strftime('%Y%m%d')}.log")
        
        logging.basicConfig(
            level=logging.DEBUG,
            format='[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_settings(self):
        """بارگذاری تنظیمات با اضافه شدن تنظیمات تعدیل"""
        default = {
            "data_url": "https://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx?h=0&r=0",
            "client_url": "https://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{inscode}",
            "price_url": "https://cdn.tsetmc.com/api/ClosingPrice/GetChartData/{inscode}/D",
            "dollar_url": "https://dashboard-api.tgju.org/v1/tv2/history?symbol=price_dollar_rl&resolution=1D",
            "gold_url": "https://dashboard-api.tgju.org/v1/tv2/history?symbol=ons&resolution=1D",
            "output_dir": ".",
            "remove_block_trades": True,
            "apply_adjustment": True,  # ✅ تنظیم جدید: اعمال تعدیل روی داده‌ها
            "selected_columns": DEFAULT_OUTPUT_COLUMNS,
            "column_order": DEFAULT_OUTPUT_COLUMNS,
            "default_markets": ["300", "303", "309", "313", "400", "403", "404"]
        }
        
        if os.path.exists(self.settings_file):
            try:
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    # ادغام با پیش‌فرض
                    for key in default:
                        if key not in loaded:
                            loaded[key] = default[key]
                    return loaded
            except Exception as e:
                self.logger.error(f"خطا در بارگذاری تنظیمات از فایل: {e}")
                return default
        return default
    
    def save_settings(self):
        """ذخیره تنظیمات"""
        try:
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(self.settings, f, ensure_ascii=False, indent=2)
            self.logger.info("تنظیمات با موفقیت ذخیره شد")
            return True
        except Exception as e:
            self.logger.error(f"خطا در ذخیره تنظیمات: {e}")
            return False
    
    def get_setting(self, key, default=None):
        """دریافت مقدار یک تنظیم"""
        return self.settings.get(key, default)
    
    def set_setting(self, key, value):
        """تنظیم مقدار یک تنظیم"""
        self.settings[key] = value
    
    def reset_to_defaults(self):
        """بازنشانی تنظیمات به مقادیر پیش‌فرض"""
        self.settings = self.load_settings()
        if self.save_settings():
            self.logger.info("تنظیمات به مقادیر پیش‌فرض بازنشانی شد")
            return True
        return False
    
    def update_output_columns(self, new_columns):
        """به‌روزرسانی ستون‌های خروجی"""
        if isinstance(new_columns, list):
            self.settings["selected_columns"] = new_columns
            self.settings["column_order"] = new_columns
            return True
        return False
    
    def get_adjustment_url(self, inscode):
        """دریافت URL داده‌های تعدیل"""
        return f"https://cdn.tsetmc.com/api/Instrument/GetInstrumentShareChange/{inscode}"
    
    def get_adjustment_status(self):
        """دریافت وضعیت تنظیمات تعدیل"""
        return self.settings.get("apply_adjustment", True)
    
    def set_adjustment_status(self, status):
        """تنظیم وضعیت تعدیل"""
        self.settings["apply_adjustment"] = bool(status)
        self.save_settings()
    
    def export_settings(self, filepath):
        """صادرات تنظیمات به فایل"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.settings, f, ensure_ascii=False, indent=2)
            self.logger.info(f"تنظیمات به {filepath} صادر شد")
            return True
        except Exception as e:
            self.logger.error(f"خطا در صادرات تنظیمات: {e}")
            return False
    
    def import_settings(self, filepath):
        """واردات تنظیمات از فایل"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                imported = json.load(f)
            
            # ادغام تنظیمات وارد شده با تنظیمات فعلی
            for key, value in imported.items():
                self.settings[key] = value
            
            self.save_settings()
            self.logger.info(f"تنظیمات از {filepath} وارد شد")
            return True
        except Exception as e:
            self.logger.error(f"خطا در واردات تنظیمات: {e}")
            return False

# توابع کمکی
def get_industry_name(code):
    """دریافت نام صنعت بر اساس کد"""
    return INDUSTRY_MAP.get(str(code), f"نامشخص ({code})")

def get_market_label(code):
    """دریافت برچسب بازار بر اساس کد"""
    return MARKET_LABELS.get(str(code), f"نامشخص ({code})")

def create_default_config():
    """ایجاد یک نمونه پیکربندی پیش‌فرض"""
    config = Config()
    return config

if __name__ == "__main__":
    # تست اجرای مستقل
    config = Config()
    
    print("تنظیمات بارگذاری شد:")
    for key, value in config.settings.items():
        if key not in ["selected_columns", "column_order"]:
            print(f"  {key}: {value}")
    
    print(f"\nتعداد ستون‌های پیش‌فرض: {len(config.settings['selected_columns'])}")
    print(f"اعمال تعدیل فعال: {config.get_adjustment_status()}")
    
    # تست تغییر تنظیمات
    config.set_setting("output_dir", "./test_output")
    config.set_adjustment_status(False)
    
    print(f"\nپس از تغییرات:")
    print(f"  مسیر خروجی: {config.get_setting('output_dir')}")
    print(f"  اعمال تعدیل: {config.get_adjustment_status()}")
    
    # ذخیره تنظیمات
    if config.save_settings():
        print("\nتنظیمات با موفقیت ذخیره شد")
    
    # تست توابع کمکی
    print(f"\nنمونه توابع کمکی:")
    print(f"  صنعت کد '43': {get_industry_name('43')}")
    print(f"  بازار کد '300': {get_market_label('300')}")
    print(f"  بازار کد '999': {get_market_label('999')}")