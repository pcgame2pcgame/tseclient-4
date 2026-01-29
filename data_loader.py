# data_loader.py
import requests
import pandas as pd
import re
import logging
from datetime import datetime
from config import INDUSTRY_MAP, MARKET_LABELS

class DataLoader:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # داده‌ها
        self.raw_data = None
        self.filtered_data = None
        self.market_codes = []
        self.industries = []
        self.symbols = []
        
        # داده‌های جدید: دلار و طلا
        self.dollar_data = None
        self.gold_data = None
        
        # فیلترها
        self.selected_markets = []
        self.selected_industries = []
        self.selected_symbols = []
    
    def normalize_text(self, text):
        """نرمال‌سازی متن فارسی"""
        if pd.isna(text):
            return ''
        
        text = str(text)
        
        # تبدیل حروف عربی به فارسی
        arabic_to_persian = {
            'ي': 'ی', 'ى': 'ی', 'ك': 'ک', 'ﺊ': 'ی', 'ﺋ': 'ی',
            'ﺌ': 'ی', 'ﻲ': 'ی', 'ﻳ': 'ی', 'ﻴ': 'ی', 'ﻚ': 'ک',
            'ﻛ': 'ک', 'ﻜ': 'ک', 'ﺆ': 'و', 'ﺂ': 'آ', 'ﺄ': 'ا',
            'ﺈ': 'ا', 'ﺎ': 'ا', 'ﺍ': 'ا', 'ﺑ': 'ب', 'ﺒ': 'ب',
            'ﺐ': 'ب', 'ﺏ': 'ب', 'ﺗ': 'ت', 'ﺘ': 'ت', 'ﺖ': 'ت',
            'ﺕ': 'ت', 'ﺜ': 'ث', 'ﺚ': 'ث', 'ﺛ': 'ث', 'ﺟ': 'ج',
            'ﺠ': 'ج', 'ﺞ': 'ج', 'ﺝ': 'ج', 'ﺣ': 'ح', 'ﺤ': 'ح',
            'ﺢ': 'ح', 'ﺡ': 'ح', 'ﺧ': 'خ', 'ﺨ': 'خ', 'ﺦ': 'خ',
            'ﺥ': 'خ', 'ﺳ': 'س', 'ﺴ': 'س', 'ﺲ': 'س', 'ﺱ': 'س',
            'ﺷ': 'ش', 'ﺸ': 'ش', 'ﺶ': 'ش', 'ﺵ': 'ش', 'ﺻ': 'ص',
            'ﺼ': 'ص', 'ﺺ': 'ص', 'ﺹ': 'ص', 'ﺿ': 'ض', 'ﻀ': 'ض',
            'ﺾ': 'ض', 'ﺽ': 'ض', 'ﻃ': 'ط', 'ﻄ': 'ط', 'ﻂ': 'ط',
            'ﻁ': 'ط', 'ﻇ': 'ظ', 'ﻈ': 'ظ', 'ﻆ': 'ظ', 'ﻅ': 'ظ',
            'ﻋ': 'ع', 'ﻌ': 'ع', 'ﻊ': 'ع', 'ﻉ': 'ع', 'ﻏ': 'غ',
            'ﻐ': 'غ', 'ﻎ': 'غ', 'ﻍ': 'غ', 'ﻓ': 'ف', 'ﻔ': 'ف',
            'ﻒ': 'ف', 'ﻑ': 'ف', 'ﻗ': 'ق', 'ﻘ': 'ق', 'ﻖ': 'ق',
            'ﻕ': 'ق', 'ﻛ': 'ک', 'ﻜ': 'ک', 'ﻚ': 'ک', 'ﮐ': 'ک',
            'ﮑ': 'ک', 'ﻙ': 'ک', 'ﻟ': 'ل', 'ﻠ': 'ل', 'ﻞ': 'ل',
            'ﻝ': 'ل', 'ﻣ': 'م', 'ﻤ': 'م', 'ﻢ': 'م', 'ﻡ': 'م',
            'ﻧ': 'ن', 'ﻨ': 'ن', 'ﻦ': 'ن', 'ﻥ': 'ن', 'ﻭ': 'و',
            'ﻮ': 'و', 'ﻫ': 'ه', 'ﻬ': 'ه', 'ﻪ': 'ه', 'ﻩ': 'ه',
            'ﻳ': 'ی', 'ﻴ': 'ی', 'ﻲ': 'ی', 'ﻱ': 'ی', 'ﻯ': 'ی',
            'ﯾ': 'ی', 'ﯿ': 'ی', 'ﯽ': 'ی', 'ﯼ': 'ی', 'ﺀ': '',
            'ﺁ': 'آ', 'ﺃ': 'ا', 'ﺅ': 'و', 'ﺇ': 'ا', 'ﺉ': 'ی',
            'ﺊ': 'ی', 'ﺋ': 'ی', 'ﺌ': 'ی', 'ﺎ': 'ا'
        }
        
        for arabic, persian in arabic_to_persian.items():
            text = text.replace(arabic, persian)
        
        # تبدیل ارقام فارسی/عربی به انگلیسی
        persian_digits = str.maketrans('۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩', '01234567890123456789')
        text = text.translate(persian_digits)
        
        # حذف نیم‌فاصله و فضاهای اضافی
        text = text.replace('\u200c', ' ').replace('\u200d', ' ').strip()
        text = re.sub(r'\s+', ' ', text)
        
        return text
    
    def _interpolate_price(self, df, target_date_str, price_column):
        """درونیابی قیمت برای تاریخ مورد نظر"""
        try:
            if df is None or df.empty:
                return 0
            
            # تبدیل تاریخ هدف به عدد
            target_date = int(target_date_str)
            
            # یافتن رکوردهای قبل و بعد
            df['date_int'] = df['recDate'].astype(int)
            df = df.sort_values('date_int')
            
            # یافتن رکورد دقیق
            exact_match = df[df['date_int'] == target_date]
            if not exact_match.empty:
                return int(exact_match.iloc[0][price_column])
            
            # یافتن رکوردهای قبل و بعد
            before = df[df['date_int'] < target_date]
            after = df[df['date_int'] > target_date]
            
            if before.empty and after.empty:
                return 0
            elif before.empty:
                # فقط بعد وجود دارد - از اولین رکورد بعدی استفاده می‌کنیم
                return int(after.iloc[0][price_column])
            elif after.empty:
                # فقط قبل وجود دارد - از آخرین رکورد قبلی استفاده می‌کنیم
                return int(before.iloc[-1][price_column])
            else:
                # هر دو وجود دارند - درونیابی خطی
                before_row = before.iloc[-1]
                after_row = after.iloc[0]
                
                date_before = before_row['date_int']
                date_after = after_row['date_int']
                price_before = before_row[price_column]
                price_after = after_row[price_column]
                
                # محاسبه درونیابی خطی
                if date_after == date_before:
                    return int(price_before)
                
                # نسبت فاصله
                ratio = (target_date - date_before) / (date_after - date_before)
                interpolated_price = price_before + ratio * (price_after - price_before)
                return int(interpolated_price)
                
        except Exception as e:
            self.logger.warning(f"خطا در درونیابی قیمت برای تاریخ {target_date_str}: {e}")
            return 0
    
    def fetch_dollar_data(self):
        """دریافت داده قیمت دلار"""
        try:
            url = self.config.settings["dollar_url"]
            self.logger.info(f"دریافت داده دلار از: {url}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('s') == 'ok':
                records = []
                num_records = len(data['t'])
                
                for i in range(num_records):
                    timestamp = data['t'][i]
                    date_obj = datetime.fromtimestamp(timestamp)
                    rec_date = date_obj.strftime('%Y%m%d')
                    
                    if (i < len(data['o']) and i < len(data['c']) and 
                        i < len(data['h']) and i < len(data['l'])):
                        
                        volume = data['v'][i] if 'v' in data and i < len(data['v']) and data['v'][i] is not None else 0
                        
                        record = {
                            'recDate': rec_date,
                            'dollar_open': int(float(data['o'][i])),
                            'dollar_close': int(float(data['c'][i])),
                            'dollar_high': int(float(data['h'][i])),
                            'dollar_low': int(float(data['l'][i])),
                            'dollar_volume': int(float(volume))
                        }
                        records.append(record)
                
                df = pd.DataFrame(records)
                df = df.sort_values('recDate')
                self.dollar_data = df
                
                self.logger.info(f"داده دلار بارگذاری شد: {len(df)} رکورد")
                return True, f"داده دلار: {len(df)} رکورد"
            else:
                self.logger.error("خطا در دریافت داده دلار: وضعیت پاسخ نا‌موفق")
                return False, "وضعیت پاسخ دلار نا‌موفق"
                
        except Exception as e:
            self.logger.error(f"خطا در دریافت داده دلار: {e}")
            return False, str(e)
    
    def fetch_gold_data(self):
        """دریافت داده قیمت انس طلا"""
        try:
            url = self.config.settings["gold_url"]
            self.logger.info(f"دریافت داده طلا از: {url}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('s') == 'ok':
                records = []
                num_records = len(data['t'])
                
                for i in range(num_records):
                    timestamp = data['t'][i]
                    date_obj = datetime.fromtimestamp(timestamp)
                    rec_date = date_obj.strftime('%Y%m%d')
                    
                    if (i < len(data['o']) and i < len(data['c']) and 
                        i < len(data['h']) and i < len(data['l'])):
                        
                        volume = data['v'][i] if 'v' in data and i < len(data['v']) and data['v'][i] is not None else 0
                        
                        record = {
                            'recDate': rec_date,
                            'gold_open': int(float(data['o'][i])),
                            'gold_close': int(float(data['c'][i])),
                            'gold_high': int(float(data['h'][i])),
                            'gold_low': int(float(data['l'][i])),
                            'gold_volume': int(float(volume))
                        }
                        records.append(record)
                
                df = pd.DataFrame(records)
                df = df.sort_values('recDate')
                self.gold_data = df
                
                self.logger.info(f"داده طلا بارگذاری شد: {len(df)} رکورد")
                return True, f"داده طلا: {len(df)} رکورد"
            else:
                self.logger.error("خطا در دریافت داده طلا: وضعیت پاسخ نا‌موفق")
                return False, "وضعیت پاسخ طلا نا‌موفق"
                
        except Exception as e:
            self.logger.error(f"خطا در دریافت داده طلا: {e}")
            return False, str(e)
    
    def fetch_external_data(self):
        """دریافت داده‌های خارجی (دلار و طلا)"""
        results = []
        
        # دریافت داده دلار
        dollar_success, dollar_message = self.fetch_dollar_data()
        results.append(f"دلار: {'✅' if dollar_success else '❌'} {dollar_message}")
        
        # دریافت داده طلا
        gold_success, gold_message = self.fetch_gold_data()
        results.append(f"طلا: {'✅' if gold_success else '❌'} {gold_message}")
        
        return all([dollar_success, gold_success]), " | ".join(results)
    
    def get_dollar_price(self, rec_date):
        """دریافت قیمت دلار برای تاریخ مشخص با درونیابی"""
        if self.dollar_data is None:
            return 0
        
        try:
            date_str = str(rec_date)
            
            # ابتدا سعی می‌کنیم قیمت دقیق را پیدا کنیم
            match = self.dollar_data[self.dollar_data['recDate'] == date_str]
            if not match.empty:
                return int(match.iloc[0]['dollar_close'])
            
            # اگر پیدا نشد، درونیابی می‌کنیم
            return self._interpolate_price(self.dollar_data, date_str, 'dollar_close')
            
        except Exception as e:
            self.logger.warning(f"خطا در دریافت قیمت دلار برای تاریخ {rec_date}: {e}")
            return 0
    
    def get_gold_price(self, rec_date):
        """دریافت قیمت طلا برای تاریخ مشخص با درونیابی"""
        if self.gold_data is None:
            return 0
        
        try:
            date_str = str(rec_date)
            
            # ابتدا سعی می‌کنیم قیمت دقیق را پیدا کنیم
            match = self.gold_data[self.gold_data['recDate'] == date_str]
            if not match.empty:
                return int(match.iloc[0]['gold_close'])
            
            # اگر پیدا نشد، درونیابی می‌کنیم
            return self._interpolate_price(self.gold_data, date_str, 'gold_close')
            
        except Exception as e:
            self.logger.warning(f"خطا در دریافت قیمت طلا برای تاریخ {rec_date}: {e}")
            return 0
    
    def calculate_thousand_dollar(self, dollar_price, pl_price):
        """محاسبه ستون 1000 دلار"""
        if dollar_price == 0 or pl_price == 0:
            return 0
        
        try:
            # (دلار * 1000) / pl و گرد کردن به پایین
            result = (dollar_price * 1000) // pl_price
            return int(result)
        except:
            return 0
    
    def calculate_one_ounce(self, gold_price, dollar_price, pl_price):
        """محاسبه ستون 1 انس"""
        if gold_price == 0 or dollar_price == 0 or pl_price == 0:
            return 0
        
        try:
            # (طلا * دلار) / pl و گرد کردن به پایین
            result = (gold_price * dollar_price) // pl_price
            return int(result)
        except:
            return 0
    
    def fetch_data(self):
        """دریافت داده از TSETMC"""
        try:
            url = self.config.settings["data_url"]
            self.logger.info(f"دریافت داده از: {url}")
            
            response = requests.get(url, timeout=30)
            response.encoding = 'utf-8'
            text = response.text
            
            if not text:
                return False, "داده‌ای دریافت نشد"
            
            # تقسیم به بخش‌ها
            sections = text.split('@')
            self.logger.info(f"تعداد بخش‌ها: {len(sections)}")
            
            if len(sections) < 3:
                return False, "داده ناقص است"
            
            # پردازش بخش 2 (اطلاعات اصلی)
            section2 = sections[2]
            rows = [r.strip() for r in section2.split(';') if r.strip()]
            
            if not rows:
                return False, "هیچ ردیفی در بخش 2 وجود ندارد"
            
            # ساخت DataFrame
            data = []
            field_mapping = {
                0: "کد_داخلی",
                1: "کد_بین_المللی",
                2: "نماد",
                3: "نام_شرکت",
                4: "زمان_آخرین_معامله",
                5: "اولین_قیمت",
                6: "قیمت_پایانی",
                7: "قیمت_آخرین_معامله",
                8: "تعداد_معاملات",
                9: "حجم_معاملات",
                10: "ارزش_معاملات",
                11: "کمترین_قیمت",
                12: "بیشترین_قیمت",
                13: "قیمت_دیروز",
                14: "EPS",
                15: "حجم_مبنا",
                16: "تعداد_بازدید_کننده",
                17: "بازار_اصلی",
                18: "گروه_صنعت",
                19: "حداکثر_قیمت_مجاز",
                20: "حداقل_قیمت_مجاز",
                21: "تعداد_کل_سهام",
                22: "کد_بازار",
                23: "NAV",
                24: "موقعیت_های_باز",
                25: "دسته_بندی_تخصصی"
            }
            
            for i, row in enumerate(rows):
                fields = row.split(',')
                record = {'ردیف': i + 1}
                
                for idx, name in field_mapping.items():
                    if idx < len(fields):
                        value = fields[idx]
                        # نرمال‌سازی متن
                        if name in ['نماد', 'نام_شرکت', 'گروه_صنعت']:
                            value = self.normalize_text(value)
                        record[name] = value
                    else:
                        record[name] = ''
                
                data.append(record)
            
            self.raw_data = pd.DataFrame(data)
            self.filtered_data = self.raw_data.copy()
            
            # اضافه کردن ستون نام صنعت
            if 'گروه_صنعت' in self.raw_data.columns:
                self.raw_data['نام_صنعت'] = self.raw_data['گروه_صنعت'].apply(
                    lambda x: INDUSTRY_MAP.get(str(x).strip(), str(x))
                )
                self.filtered_data['نام_صنعت'] = self.filtered_data['گروه_صنعت'].apply(
                    lambda x: INDUSTRY_MAP.get(str(x).strip(), str(x))
                )
            
            self.logger.info(f"داده بارگذاری شد: {len(self.raw_data)} نماد")
            
            # دریافت داده‌های خارجی (دلار و طلا)
            external_success, external_message = self.fetch_external_data()
            if external_success:
                self.logger.info(f"داده‌های خارجی: {external_message}")
            else:
                self.logger.warning(f"خطا در دریافت داده‌های خارجی: {external_message}")
            
            return True, f"{len(self.raw_data)} نماد بارگذاری شد"
            
        except Exception as e:
            self.logger.error(f"خطا در دریافت داده: {e}")
            return False, str(e)
    
    def get_market_codes(self):
        """دریافت لیست کدهای بازار"""
        if self.raw_data is None or 'کد_بازار' not in self.raw_data.columns:
            return []
        
        market_counts = self.raw_data['کد_بازار'].value_counts()
        result = []
        
        for code, count in market_counts.items():
            code_str = str(code)
            label = MARKET_LABELS.get(code_str, 'نامشخص')
            bold = code_str in ['300', '303', '309', '313', '400', '403', '404']
            default_selected = code_str in self.config.settings.get('default_markets', [])
            
            result.append({
                'code': code_str,
                'label': label,
                'count': int(count),
                'bold': bold,
                'default_selected': default_selected
            })
        
        # مرتب‌سازی بر اساس کد
        result.sort(key=lambda x: x['code'])
        return result
    
    def get_industries(self):
        """دریافت لیست صنایع"""
        if self.raw_data is None or 'گروه_صنعت' not in self.raw_data.columns:
            return []
        
        industries = []
        
        # گروه‌بندی بر اساس کد صنعت
        for code, name in INDUSTRY_MAP.items():
            count = len(self.raw_data[self.raw_data['گروه_صنعت'] == code])
            if count > 0:
                industries.append({
                    'code': code,
                    'name': name,
                    'count': count
                })
        
        # اضافه کردن صنایع دیگر که در نگاشت نیستند
        unique_codes = self.raw_data['گروه_صنعت'].unique()
        for code in unique_codes:
            code_str = str(code)
            if code_str not in INDUSTRY_MAP and code_str.strip():
                count = len(self.raw_data[self.raw_data['گروه_صنعت'] == code])
                industries.append({
                    'code': code_str,
                    'name': code_str,
                    'count': count
                })
        
        # مرتب‌سازی بر اساس نام
        industries.sort(key=lambda x: x['name'])
        return industries
    
    def apply_market_filter(self, selected_codes, remove_block_trades=True):
        """اعمال فیلتر بازار"""
        if self.raw_data is None:
            return False, "داده‌ای وجود ندارد"
        
        self.selected_markets = selected_codes
        
        # فیلتر بر اساس کد بازار
        self.filtered_data = self.raw_data[
            self.raw_data['کد_بازار'].astype(str).isin(selected_codes)
        ].copy()
        
        # حذف معاملات بلوکی
        if remove_block_trades:
            # نمادهایی که شناسه بین‌المللی به 0001 ختم نمی‌شود
            mask = self.filtered_data['کد_بین_المللی'].astype(str).str.endswith('0001')
            # همچنین حذف نمادهایی که با عدد پایان می‌یابند
            mask = mask & ~self.filtered_data['نماد'].astype(str).str[-1].str.isdigit()
            self.filtered_data = self.filtered_data[mask]
        
        self.logger.info(f"پس از فیلتر بازار: {len(self.filtered_data)} نماد")
        return True, f"{len(self.filtered_data)} نماد"
    
    def apply_industry_filter(self, selected_industries):
        """اعمال فیلتر صنعت"""
        if self.filtered_data is None:
            return False, "ابتدا فیلتر بازار را اعمال کنید"
        
        self.selected_industries = selected_industries
        
        if selected_industries:
            self.filtered_data = self.filtered_data[
                self.filtered_data['گروه_صنعت'].astype(str).isin(selected_industries)
            ].copy()
        
        self.logger.info(f"پس از فیلتر صنعت: {len(self.filtered_data)} نماد")
        return True, f"{len(self.filtered_data)} نماد"
    
    def get_symbols(self):
        """دریافت لیست نمادها"""
        if self.filtered_data is None:
            return []
        
        symbols = []
        for idx, row in self.filtered_data.iterrows():
            symbols.append({
                'ردیف': idx + 1,
                'نماد': row.get('نماد', ''),
                'نام_شرکت': row.get('نام_شرکت', ''),
                'کد_بین_المللی': row.get('کد_بین_المللی', ''),
                'کد_داخلی': row.get('کد_داخلی', ''),
                'کد_بازار': row.get('کد_بازار', ''),
                'گروه_صنعت': row.get('گروه_صنعت', ''),
                'نام_صنعت': row.get('نام_صنعت', '')
            })
        
        return symbols
    
    def get_symbol_info(self, symbol):
        """دریافت اطلاعات کامل یک نماد"""
        if self.filtered_data is None:
            return None
        
        symbol_data = self.filtered_data[self.filtered_data['نماد'] == symbol]
        if not symbol_data.empty:
            return symbol_data.iloc[0]
        return None