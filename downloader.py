# downloader.py
import requests
import pandas as pd
import json
import time
import os
import re
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple, Optional, Any
import concurrent.futures
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

class Downloader:
    def __init__(self, config, data_loader):
        self.config = config
        self.data_loader = data_loader
        self.logger = logging.getLogger(__name__)
        self.is_downloading = False
        self.session = None
        self.cache_dir = "cache"
        self.setup_cache()
        
        # تنظیمات دانلود
        self.max_retries = 3
        self.timeout = 30
        self.delay_between_requests = 0.1  # ثانیه
        self.max_workers = 5  # حداکثر thread برای دانلود موازی
        
        # آمار دانلود
        self.download_stats = {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'start_time': None,
            'end_time': None
        }
    
    def setup_cache(self):
        """راه‌اندازی سیستم کش"""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
    
    def get_session(self):
        """دریافت session برای اتصالات مکرر"""
        if self.session is None:
            self.session = requests.Session()
            # تنظیم headers برای شبیه‌سازی مرورگر
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
            })
        return self.session
    
    def close_session(self):
        """بستن session"""
        if self.session:
            self.session.close()
            self.session = None
    
    def _retry_request(self, url, max_retries=None, method='GET', **kwargs):
        """درخواست با قابلیت تلاش مجدد"""
        if max_retries is None:
            max_retries = self.max_retries
        
        session = self.get_session()
        
        for attempt in range(max_retries):
            try:
                if method.upper() == 'GET':
                    response = session.get(url, timeout=self.timeout, **kwargs)
                elif method.upper() == 'POST':
                    response = session.post(url, timeout=self.timeout, **kwargs)
                else:
                    raise ValueError(f"Method {method} not supported")
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout در تلاش {attempt + 1}/{max_retries} برای {url}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # افزایش تاخیر به صورت نمایی
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"خطا در تلاش {attempt + 1}/{max_retries} برای {url}: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(1)
        
        return None
    
    def _get_cached_data(self, cache_key: str, expiration_hours: int = 24) -> Optional[Any]:
        """دریافت داده از کش"""
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                # بررسی تاریخ انقضا
                cache_time = datetime.fromisoformat(cache_data.get('timestamp', '2000-01-01'))
                if datetime.now() - cache_time < timedelta(hours=expiration_hours):
                    return cache_data.get('data')
                else:
                    self.logger.debug(f"داده کش {cache_key} منقضی شده است")
                    os.remove(cache_file)
                    
            except Exception as e:
                self.logger.warning(f"خطا در خواندن کش {cache_key}: {e}")
        
        return None
    
    def _save_to_cache(self, cache_key: str, data: Any):
        """ذخیره داده در کش"""
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        
        try:
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            self.logger.warning(f"خطا در ذخیره کش {cache_key}: {e}")
    
    def _deven_to_yyyymmdd(self, deven: int) -> str:
        """تبدیل dEven به تاریخ میلادی YYYYMMDD"""
        try:
            if isinstance(deven, (int, float)) and deven > 0:
                # فرض: dEven تعداد ثانیه از 1970-01-01 است
                timestamp = int(deven)
                base_date = datetime(1970, 1, 1)
                target_date = base_date + timedelta(seconds=timestamp)
                return target_date.strftime("%Y%m%d")
            else:
                return "00000000"
        except Exception as e:
            self.logger.error(f"خطا در تبدیل dEven {deven}: {e}")
            return "00000000"
    
    def _normalize_rec_date(self, rec_date) -> str:
        """نرمال‌سازی recDate به YYYYMMDD"""
        if rec_date is None:
            return ""
        
        try:
            if isinstance(rec_date, (int, float)):
                rec_date = str(int(rec_date))
            
            # بررسی فرمت ISO (2023-12-31T00:00:00)
            if isinstance(rec_date, str) and '-' in rec_date and 'T' in rec_date:
                try:
                    date_part = rec_date.split('T')[0]
                    dt = datetime.strptime(date_part, "%Y-%m-%d")
                    return dt.strftime("%Y%m%d")
                except ValueError:
                    pass
            
            # بررسی فرمت YYYYMMDD
            if isinstance(rec_date, str) and len(rec_date) == 8 and rec_date.isdigit():
                # اعتبارسنجی تاریخ
                try:
                    year = int(rec_date[:4])
                    month = int(rec_date[4:6])
                    day = int(rec_date[6:8])
                    datetime(year, month, day)
                    return rec_date
                except ValueError:
                    pass
            
            # سایر فرمت‌ها
            if isinstance(rec_date, str):
                # حذف کاراکترهای غیرعددی
                digits = ''.join(filter(str.isdigit, rec_date))
                if len(digits) >= 8:
                    return digits[:8]
            
            return str(rec_date)
            
        except Exception as e:
            self.logger.warning(f"خطا در نرمال‌سازی تاریخ {rec_date}: {e}")
            return ""
    
    def _miladi_to_shamsi_yyyymmdd(self, rec_date) -> str:
        """تبدیل تاریخ میلادی به شمسی"""
        try:
            miladi_yyyymmdd = self._normalize_rec_date(rec_date)
            if not miladi_yyyymmdd or len(miladi_yyyymmdd) != 8:
                return ""
            
            year = int(miladi_yyyymmdd[:4])
            month = int(miladi_yyyymmdd[4:6])
            day = int(miladi_yyyymmdd[6:8])
            
            # بررسی اعتبار تاریخ میلادی
            try:
                datetime(year, month, day)
            except ValueError:
                return miladi_yyyymmdd
            
            # تبدیل به تاریخ شمسی
            try:
                import jdatetime
                shamsi_date = jdatetime.date.fromgregorian(
                    year=year, month=month, day=day
                )
                return shamsi_date.strftime("%Y%m%d")
            except ImportError:
                self.logger.warning("کتابخانه jdatetime نصب نیست. تاریخ شمسی محاسبه نمی‌شود.")
                return miladi_yyyymmdd
            except Exception as e:
                self.logger.warning(f"خطا در تبدیل به تاریخ شمسی: {e}")
                return miladi_yyyymmdd
                
        except Exception as e:
            self.logger.error(f"خطا در تبدیل تاریخ میلادی به شمسی: {e}")
            return ""
    
    def _check_volume_match(self, client_volume, price_volume, threshold=0.95) -> bool:
        """بررسی تطابق حجم‌ها با آستانه قابل تنظیم"""
        if client_volume == 0 and price_volume == 0:
            return True
        
        if price_volume == 0 or client_volume == 0:
            # اگر یکی صفر باشد و دیگری نباشد، تطابق ندارند
            return False
        
        # نسبت تطابق
        ratio = min(client_volume, price_volume) / max(client_volume, price_volume)
        return ratio >= threshold
    
    def _find_best_alignment(self, client_items, price_records, symbol) -> Tuple[List, List]:
        """یافتن بهترین تطابق بین دو لیست با استفاده از الگوریتم تطابق"""
        if not client_items or not price_records:
            return [], []
        
        client_len = len(client_items)
        price_len = len(price_records)
        
        # اگر طول لیست‌ها خیلی متفاوت است، هشدار بده
        if abs(client_len - price_len) > min(client_len, price_len) * 0.5:  # بیش از 50% اختلاف
            self.logger.warning(f"اختلاف طول زیاد برای {symbol}: client={client_len}, price={price_len}")
        
        # ماتریس تطابق
        match_matrix = []
        for i, client_item in enumerate(client_items[:100]):  # محدود کردن برای کارایی
            row = []
            client_vol = client_item.get('sell_N_Volume', 0) + client_item.get('sell_I_Volume', 0)
            
            for j, price_item in enumerate(price_records[:100]):
                price_vol = price_item.get('qTotTran5J', 0)
                
                if self._check_volume_match(client_vol, price_vol, 0.9):  # آستانه 90% برای تطابق اولیه
                    row.append(1)
                else:
                    row.append(0)
            match_matrix.append(row)
        
        # الگوریتم تطابق ساده - پیدا کردن اولین تطابق‌های معقول
        matched_client = []
        matched_price = []
        
        i, j = 0, 0
        max_offset = 5  # حداکثر جستجو برای تطابق
        
        while i < client_len and j < price_len:
            client_item = client_items[i]
            price_item = price_records[j]
            
            client_vol = client_item.get('sell_N_Volume', 0) + client_item.get('sell_I_Volume', 0)
            price_vol = price_item.get('qTotTran5J', 0)
            
            if self._check_volume_match(client_vol, price_vol, 0.95):
                matched_client.append(client_item)
                matched_price.append(price_item)
                i += 1
                j += 1
            else:
                # جستجوی محدود برای تطابق
                found = False
                for offset in range(1, max_offset + 1):
                    # بررسی جلوتر در client
                    if i + offset < client_len:
                        next_client_vol = client_items[i + offset].get('sell_N_Volume', 0) + \
                                        client_items[i + offset].get('sell_I_Volume', 0)
                        if self._check_volume_match(next_client_vol, price_vol, 0.95):
                            # حذف از client
                            for _ in range(offset):
                                self.logger.debug(f"حذف ردیف {i} از client برای {symbol}")
                                i += 1
                            matched_client.append(client_items[i])
                            matched_price.append(price_item)
                            i += 1
                            j += 1
                            found = True
                            break
                    
                    # بررسی جلوتر در price
                    if j + offset < price_len:
                        next_price_vol = price_records[j + offset].get('qTotTran5J', 0)
                        if self._check_volume_match(client_vol, next_price_vol, 0.95):
                            # حذف از price
                            for _ in range(offset):
                                self.logger.debug(f"حذف ردیف {j} از price برای {symbol}")
                                j += 1
                            matched_client.append(client_item)
                            matched_price.append(price_records[j])
                            i += 1
                            j += 1
                            found = True
                            break
                
                if not found:
                    # اگر تطابقی پیدا نشد، هر دو را رد کنیم
                    self.logger.debug(f"رد ردیف‌های {i},{j} برای {symbol}")
                    i += 1
                    j += 1
        
        # اعتبارسنجی تطابق‌های یافت شده
        if matched_client and matched_price:
            self._validate_matches(matched_client, matched_price, symbol)
        
        return matched_client, matched_price
    
    def _validate_matches(self, client_items, price_records, symbol):
        """اعتبارسنجی تطابق‌های یافت شده"""
        if len(client_items) != len(price_records):
            self.logger.warning(f"طول لیست‌های تطبیق‌یافته برای {symbol} برابر نیست")
            return
        
        # بررسی کیفیت تطابق‌ها
        perfect_matches = 0
        good_matches = 0
        total = len(client_items)
        
        for i in range(total):
            client_vol = client_items[i].get('sell_N_Volume', 0) + client_items[i].get('sell_I_Volume', 0)
            price_vol = price_records[i].get('qTotTran5J', 0)
            
            if self._check_volume_match(client_vol, price_vol, 0.98):
                perfect_matches += 1
            elif self._check_volume_match(client_vol, price_vol, 0.95):
                good_matches += 1
        
        if total > 0:
            perfect_percent = (perfect_matches / total) * 100
            good_percent = (good_matches / total) * 100
            
            self.logger.info(f"نماد {symbol}: تطابق عالی {perfect_percent:.1f}% ({perfect_matches}/{total})")
            self.logger.info(f"نماد {symbol}: تطابق خوب {good_percent:.1f}% ({good_matches}/{total})")
            
            if perfect_percent < 50:
                self.logger.warning(f"کیفیت تطابق برای {symbol} پایین است")
    
    def download_adjustment_data(self, internal_code: str, use_cache: bool = True) -> Optional[Dict]:
        """دانلود داده‌های تعدیل سهام"""
        cache_key = f"adjustment_{internal_code}"
        
        if use_cache:
            cached_data = self._get_cached_data(cache_key, expiration_hours=168)  # 7 روز کش
            if cached_data is not None:
                self.logger.debug(f"داده تعدیل {internal_code} از کش بازیابی شد")
                return cached_data
        
        try:
            url = f"https://cdn.tsetmc.com/api/Instrument/GetInstrumentShareChange/{internal_code}"
            self.logger.debug(f"دریافت داده تعدیل از: {url}")
            
            response = self._retry_request(url)
            
            if response is None:
                self.logger.error(f"خطا در دریافت داده تعدیل برای {internal_code}")
                return None
            
            data = response.json()
            
            # ذخیره در کش
            if use_cache and data:
                self._save_to_cache(cache_key, data)
            
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"خطا در دریافت داده تعدیل برای {internal_code}: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"خطا در پردازش JSON تعدیل برای {internal_code}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"خطای ناشناخته در دریافت داده تعدیل برای {internal_code}: {e}")
            return None
    
    def _parse_adjustment_data(self, adjustment_data: Dict) -> List[Dict]:
        """پردازش داده‌های تعدیل - اصلاح شده"""
        if not adjustment_data or 'instrumentShareChange' not in adjustment_data:
            return []
        
        adjustments = []
        for item in adjustment_data['instrumentShareChange']:
            dEven = item.get('dEven', 0)
            new_shares = float(item.get('numberOfShareNew', 1))
            old_shares = float(item.get('numberOfShareOld', 1))
            
            # فقط اگر تعداد سهام تغییر کرده باشد
            if new_shares > 0 and old_shares > 0 and abs(new_shares - old_shares) > 0.001:
                adjustment = {
                    'dEven': dEven,
                    'numberOfShareNew': new_shares,
                    'numberOfShareOld': old_shares,
                    'price_ratio': old_shares / new_shares,  # ضریب تعدیل قیمت: قدیم/جدید
                    'volume_ratio': new_shares / old_shares   # ضریب تعدیل حجم: جدید/قدیم
                }
                adjustments.append(adjustment)
        
        # مرتب‌سازی بر اساس تاریخ (جدیدترین به قدیمی)
        adjustments.sort(key=lambda x: x['dEven'], reverse=True)
        
        return adjustments

    def _get_adjustment_ratios_for_date(self, adjustments: List[Dict], target_date_str: str) -> Tuple[float, float]:
        """دریافت ضرایب تعدیل برای تاریخ مشخص"""
        price_ratio = 1.0
        volume_ratio = 1.0
        
        if not adjustments:
            return price_ratio, volume_ratio
        
        try:
            target_date = int(target_date_str)
            
            # فقط تعدیل‌هایی که تاریخ آنها بعد از تاریخ هدف است اعمال می‌شود
            # (یعنی اگر سهم در آینده افزایش سرمایه داشته، داده‌های گذشته باید تعدیل شوند)
            for adj in adjustments:
                if adj['dEven'] > target_date:  # افزایش سرمایه بعد از تاریخ معامله
                    price_ratio *= adj['price_ratio']
                    volume_ratio *= adj['volume_ratio']
                else:
                    # چون لیست مرتب شده است، بقیه قبل از تاریخ هستند
                    break
            
            return price_ratio, volume_ratio
            
        except Exception as e:
            self.logger.warning(f"خطا در محاسبه ضرایب تعدیل برای تاریخ {target_date_str}: {e}")
            return 1.0, 1.0

    def _apply_adjustment_to_prices_and_volumes(self, record: Dict, price_ratio: float, volume_ratio: float):
        """اعمال تعدیل به قیمت‌ها و حجم‌ها"""
        if price_ratio == 1.0 and volume_ratio == 1.0:
            return record
        
        try:
            # 1. تعدیل قیمت‌ها: قیمت قدیم × (سهام قدیم/سهام جدید)
            price_fields = ['pf', 'pl', 'pmin', 'pmax']
            for field in price_fields:
                if field in record and record[field] is not None and record[field] != '':
                    try:
                        original_value = float(record[field])
                        if original_value > 0:
                            adjusted_value = original_value * price_ratio
                            record[field] = int(adjusted_value)
                    except (ValueError, TypeError):
                        pass
            
            # 2. تعدیل حجم‌ها: حجم قدیم × (سهام جدید/سهام قدیم)
            volume_fields = ['vol', 'buy_I_Volume', 'buy_N_Volume', 'sell_I_Volume', 'sell_N_Volume']
            for field in volume_fields:
                if field in record and record[field] is not None and record[field] != '':
                    try:
                        original_value = float(record[field])
                        if original_value > 0:
                            adjusted_value = original_value * volume_ratio
                            record[field] = int(adjusted_value)
                    except (ValueError, TypeError):
                        pass
            
            # 3. تعدیل ارزش‌ها: باید با قیمت و حجم تعدیل شده سازگار باشد
            # ارزش = قیمت × حجم
            # پس اگر قیمت در ratio1 و حجم در ratio2 ضرب شود، ارزش در (ratio1 × ratio2) ضرب می‌شود
            # اما چون ratio1 × ratio2 = (قدیم/جدید) × (جدید/قدیم) = 1، پس ارزش تغییری نمی‌کند
            # بنابراین نیازی به تغییر ارزش‌ها نیست!
            
            # 4. اضافه کردن ستون ضریب تعدیل
            record['adjustment_ratio'] = price_ratio  # یا می‌توانیم هر دو را ذخیره کنیم
            
            return record
            
        except Exception as e:
            self.logger.error(f"خطا در اعمال تعدیل به رکورد: {e}")
            return record
    
    def download_client_type_data(self, internal_code: str, use_cache: bool = True) -> Optional[Dict]:
        """دانلود داده حقیقی/حقوقی با قابلیت کش"""
        cache_key = f"client_{internal_code}"
        
        if use_cache:
            cached_data = self._get_cached_data(cache_key, expiration_hours=6)
            if cached_data is not None:
                self.logger.debug(f"داده حقیقی/حقوقی {internal_code} از کش بازیابی شد")
                return cached_data
        
        try:
            url = self.config.settings["client_url"].format(inscode=internal_code)
            self.logger.debug(f"دریافت داده حقیقی/حقوقی از: {url}")
            
            response = self._retry_request(url)
            
            if response is None:
                self.logger.error(f"خطا در دریافت داده حقیقی/حقوقی برای {internal_code}")
                return None
            
            data = response.json()
            
            # ذخیره در کش
            if use_cache and data:
                self._save_to_cache(cache_key, data)
            
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"خطا در دریافت داده حقیقی/حقوقی برای {internal_code}: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"خطا در پردازش JSON برای {internal_code}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"خطای ناشناخته در دریافت داده حقیقی/حقوقی برای {internal_code}: {e}")
            return None
    
    def download_price_data(self, internal_code: str, use_cache: bool = True) -> Optional[Dict]:
        """دانلود داده قیمت با قابلیت کش"""
        cache_key = f"price_{internal_code}"
        
        if use_cache:
            cached_data = self._get_cached_data(cache_key, expiration_hours=6)
            if cached_data is not None:
                self.logger.debug(f"داده قیمت {internal_code} از کش بازیابی شد")
                return cached_data
        
        try:
            url = self.config.settings["price_url"].format(inscode=internal_code)
            self.logger.debug(f"دریافت داده قیمت از: {url}")
            
            response = self._retry_request(url)
            
            if response is None:
                self.logger.error(f"خطا در دریافت داده قیمت برای {internal_code}")
                return None
            
            data = response.json()
            
            # ذخیره در کش
            if use_cache and data:
                self._save_to_cache(cache_key, data)
            
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"خطا در دریافت داده قیمت برای {internal_code}: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"خطا در پردازش JSON برای {internal_code}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"خطای ناشناخته در دریافت داده قیمت برای {internal_code}: {e}")
            return None
    
    def _parse_price_data(self, price_data: Dict) -> List[Dict]:
        """پردازش داده قیمت و استخراج اطلاعات"""
        if not price_data or 'closingPriceChartData' not in price_data:
            return []
        
        chart_data = price_data['closingPriceChartData']
        parsed_records = []
        
        for item in chart_data:
            if not isinstance(item, dict):
                continue
            
            # استخراج تاریخ از dEven
            miladi_date = self._deven_to_yyyymmdd(item.get('dEven', 0))
            
            record = {
                'pDrCotVal': self._safe_float(item.get('pDrCotVal', 0)),  # قیمت پایانی
                'qTotTran5J': self._safe_int(item.get('qTotTran5J', 0)),  # حجم معاملات
                'priceFirst': self._safe_float(item.get('priceFirst', 0)),  # اولین قیمت
                'priceMin': self._safe_float(item.get('priceMin', 0)),  # کمترین قیمت
                'priceMax': self._safe_float(item.get('priceMax', 0)),  # بیشترین قیمت
                'priceYesterday': self._safe_float(item.get('priceYesterday', 0)),  # قیمت دیروز
                'priceChange': self._safe_float(item.get('priceChange', 0)),  # تغییر قیمت
                'price_date_yyyymmdd': miladi_date
            }
            
            # محاسبه قیمت پایانی تنظیم شده اگر وجود دارد
            if 'pClosing' in item:
                record['pClosing'] = self._safe_float(item.get('pClosing'))
            
            parsed_records.append(record)
        
        # معکوس کردن لیست (قدیمی‌ترین به جدیدترین)
        parsed_records.reverse()
        
        return parsed_records
    
    def _safe_int(self, value, default=0):
        """تبدیل امن به عدد صحیح"""
        try:
            if value is None:
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default
    
    def _safe_float(self, value, default=0.0):
        """تبدیل امن به عدد اعشاری"""
        try:
            if value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def _prepare_client_record(self, client_item: Dict) -> Dict:
        """آماده‌سازی رکورد حقیقی/حقوقی"""
        return {
            'recDate': client_item.get('recDate', ''),
            'buy_I_Volume': self._safe_int(client_item.get('buy_I_Volume', 0)),
            'buy_I_Value': self._safe_int(client_item.get('buy_I_Value', 0)),
            'buy_I_Count': self._safe_int(client_item.get('buy_I_Count', 0)),
            'buy_N_Volume': self._safe_int(client_item.get('buy_N_Volume', 0)),
            'buy_N_Value': self._safe_int(client_item.get('buy_N_Value', 0)),
            'buy_N_Count': self._safe_int(client_item.get('buy_N_Count', 0)),
            'sell_I_Volume': self._safe_int(client_item.get('sell_I_Volume', 0)),
            'sell_I_Value': self._safe_int(client_item.get('sell_I_Value', 0)),
            'sell_I_Count': self._safe_int(client_item.get('sell_I_Count', 0)),
            'sell_N_Volume': self._safe_int(client_item.get('sell_N_Volume', 0)),
            'sell_N_Value': self._safe_int(client_item.get('sell_N_Value', 0)),
            'sell_N_Count': self._safe_int(client_item.get('sell_N_Count', 0))
        }
    
    def _calculate_extra_metrics(self, client_record: Dict, price_record: Dict) -> Dict:
        """محاسبه متریک‌های اضافی"""
        metrics = {}
        
        # حجم کل خرید و فروش
        total_buy_volume = client_record['buy_I_Volume'] + client_record['buy_N_Volume']
        total_sell_volume = client_record['sell_I_Volume'] + client_record['sell_N_Volume']
        
        # ارزش کل خرید و فروش
        total_buy_value = client_record['buy_I_Value'] + client_record['buy_N_Value']
        total_sell_value = client_record['sell_I_Value'] + client_record['sell_N_Value']
        
        # تعداد کل خرید و فروش
        total_buy_count = client_record['buy_I_Count'] + client_record['buy_N_Count']
        total_sell_count = client_record['sell_I_Count'] + client_record['sell_N_Count']
        
        # محاسبه نسبت حقیقی به حقوقی
        if total_buy_volume > 0:
            metrics['buy_I_ratio'] = client_record['buy_I_Volume'] / total_buy_volume
            metrics['buy_N_ratio'] = client_record['buy_N_Volume'] / total_buy_volume
        
        if total_sell_volume > 0:
            metrics['sell_I_ratio'] = client_record['sell_I_Volume'] / total_sell_volume
            metrics['sell_N_ratio'] = client_record['sell_N_Volume'] / total_sell_volume
        
        # خالص حقیقی و حقوقی
        metrics['net_individual'] = client_record['buy_I_Volume'] - client_record['sell_I_Volume']
        metrics['net_institutional'] = client_record['buy_N_Volume'] - client_record['sell_N_Volume']
        
        # قدرت خرید/فروش
        if total_sell_volume > 0:
            metrics['buy_sell_ratio'] = total_buy_volume / total_sell_volume
        
        # میانگین اندازه معامله
        if total_buy_count > 0:
            metrics['avg_buy_trade_size'] = total_buy_volume / total_buy_count
        
        if total_sell_count > 0:
            metrics['avg_sell_trade_size'] = total_sell_volume / total_sell_count
        
        # تغییرات قیمت
        if 'priceYesterday' in price_record and price_record['priceYesterday'] > 0:
            metrics['price_change_percent'] = (
                (price_record.get('pDrCotVal', 0) - price_record['priceYesterday']) / 
                price_record['priceYesterday'] * 100
            )
        
        return metrics
    
    def _calculate_new_columns(self, rec_date: str, pl_price: float) -> Dict:
        """محاسبه ستون‌های جدید (دلار، طلا، 1000 دلار، 1 انس)"""
        result = {
            'dollar': 0,
            'ounces_gold': 0,
            'thousand_dollar': 0,
            'one_ounce': 0
        }
        
        try:
            # دریافت قیمت دلار برای تاریخ داده شده
            dollar_price = self.data_loader.get_dollar_price(rec_date)
            
            # دریافت قیمت طلا برای تاریخ داده شده
            gold_price = self.data_loader.get_gold_price(rec_date)
            
            # محاسبه ستون‌ها
            result['dollar'] = int(dollar_price) if dollar_price else 0
            result['ounces_gold'] = int(gold_price) if gold_price else 0
            
            # محاسبه thousand_dollar: (دلار * 1000) / pl (گرد کردن به پایین)
            if dollar_price > 0 and pl_price > 0:
                result['thousand_dollar'] = int((dollar_price * 1000) // pl_price)
            
            # محاسبه one_ounce: (طلا * دلار) / pl (گرد کردن به پایین)
            if dollar_price > 0 and gold_price > 0 and pl_price > 0:
                result['one_ounce'] = int((gold_price * dollar_price) // pl_price)
            
        except Exception as e:
            self.logger.warning(f"خطا در محاسبه ستون‌های جدید برای تاریخ {rec_date}: {e}")
        
        return result
    
    def download_symbol_data(self, symbol: str, internal_code: str, apply_adjustment: bool = True) -> Tuple[bool, Any]:
        """دانلود و ترکیب داده‌های یک نماد با منطق صحیح تعدیل"""
        try:
            self.logger.info(f"شروع دانلود داده‌های {symbol} (کد: {internal_code}) - حالت تعدیل: {apply_adjustment}")
            
            # دانلود داده‌های اصلی
            client_data = self.download_client_type_data(internal_code)
            price_data = self.download_price_data(internal_code)
            
            # بررسی دریافت داده‌ها
            if not client_data or 'clientType' not in client_data:
                self.logger.error(f"داده حقیقی/حقوقی برای {symbol} دریافت نشد")
                return False, "داده حقیقی/حقوقی دریافت نشد"
            
            if not price_data:
                self.logger.error(f"داده قیمت برای {symbol} دریافت نشد")
                return False, "داده قیمت دریافت نشد"
            
            # دریافت و پردازش داده‌های تعدیل اگر فعال باشد
            adjustments = []
            if apply_adjustment:
                adjustment_data = self.download_adjustment_data(internal_code)
                if adjustment_data and 'instrumentShareChange' in adjustment_data:
                    adjustments = self._parse_adjustment_data(adjustment_data)
                    if adjustments:
                        self.logger.info(f"داده‌های تعدیل برای {symbol} بارگذاری شد: {len(adjustments)} رکورد")
                    else:
                        self.logger.info(f"داده تعدیل برای {symbol} یافت نشد یا قابل پردازش نیست")
                else:
                    self.logger.info(f"داده تعدیل برای {symbol} در دسترس نیست")
            
            # پردازش داده‌ها
            client_items = client_data['clientType']
            price_records = self._parse_price_data(price_data)
            
            if not client_items:
                self.logger.error(f"لیست حقیقی/حقوقی برای {symbol} خالی است")
                return False, "لیست حقیقی/حقوقی خالی است"
            
            if not price_records:
                self.logger.error(f"لیست قیمت برای {symbol} خالی است")
                return False, "لیست قیمت خالی است"
            
            # تطبیق داده‌ها
            self.logger.info(f"تطبیق داده‌های {symbol} ({len(client_items)} رکورد حقیقی/حقوقی، {len(price_records)} رکورد قیمت)")
            
            matched_client, matched_price = self._find_best_alignment(client_items, price_records, symbol)
            
            if not matched_client or not matched_price:
                self.logger.error(f"تطابقی برای {symbol} یافت نشد")
                return False, "هیچ تطابقی یافت نشد"
            
            # ایجاد رکوردهای نهایی
            records = []
            min_length = min(len(matched_client), len(matched_price))
            
            for i in range(min_length):
                client_item = matched_client[i]
                price_item = matched_price[i]
                
                # آماده‌سازی رکورد حقیقی/حقوقی
                client_record = self._prepare_client_record(client_item)
                
                # ایجاد رکورد نهایی
                record = {
                    'ticker': symbol,
                    'pf': int(price_item.get('priceFirst', 0)),
                    'pl': int(price_item.get('pDrCotVal', 0)),
                    'pmin': int(price_item.get('priceMin', 0)),
                    'pmax': int(price_item.get('priceMax', 0)),
                    'vol': int(price_item.get('qTotTran5J', 0)),
                    'recDate': client_record['recDate'],
                    'jalalidate': self._miladi_to_shamsi_yyyymmdd(client_record['recDate']),
                    'buy_I_Volume': client_record['buy_I_Volume'],
                    'buy_I_Value': client_record['buy_I_Value'],
                    'buy_I_Count': client_record['buy_I_Count'],
                    'buy_N_Volume': client_record['buy_N_Volume'],
                    'buy_N_Value': client_record['buy_N_Value'],
                    'buy_N_Count': client_record['buy_N_Count'],
                    'sell_I_Volume': client_record['sell_I_Volume'],
                    'sell_I_Value': client_record['sell_I_Value'],
                    'sell_I_Count': client_record['sell_I_Count'],
                    'sell_N_Volume': client_record['sell_N_Volume'],
                    'sell_N_Value': client_record['sell_N_Value'],
                    'sell_N_Count': client_record['sell_N_Count'],
                    'price_date_iso': client_record['recDate'],
                    'insCode': internal_code
                }
                
                # اضافه کردن ستون‌های جدید (دلار، طلا، 1000 دلار، 1 انس)
                new_columns = self._calculate_new_columns(
                    client_record['recDate'], 
                    price_item.get('pDrCotVal', 0)
                )
                record.update(new_columns)
                
                # مقدار پیش‌فرض برای ضریب تعدیل
                adjustment_ratio = 1.0
                
                # اعمال تعدیل اگر فعال باشد و داده‌های تعدیل وجود داشته باشند
                if apply_adjustment and adjustments:
                    rec_date = record['recDate']
                    
                    # محاسبه ضرایب تعدیل برای این تاریخ
                    price_ratio, volume_ratio = self._get_adjustment_ratios_for_date(adjustments, rec_date)
                    
                    if price_ratio != 1.0 or volume_ratio != 1.0:
                        self.logger.debug(
                            f"اعمال تعدیل به {symbol} برای تاریخ {rec_date}: "
                            f"price×{price_ratio:.6f}, volume×{volume_ratio:.6f}"
                        )
                        
                        # اعمال تعدیل به رکورد
                        record = self._apply_adjustment_to_record(record, price_ratio, volume_ratio)
                        
                        # ذخیره ضریب تعدیل
                        adjustment_ratio = price_ratio
                
                # همیشه ستون adjustment_ratio را اضافه می‌کنیم (حتی اگر 1.0 باشد)
                record['adjustment_ratio'] = adjustment_ratio
                
                # اضافه کردن متریک‌های اضافی
                extra_metrics = self._calculate_extra_metrics(client_record, price_item)
                record.update(extra_metrics)
                
                records.append(record)
            
            # ایجاد DataFrame
            if records:
                df = pd.DataFrame(records)
                
                # ترتیب ستون‌ها - adjustment_ratio در انتها
                output_columns = [
                    "ticker", "pf", "pl", "pmin", "pmax", "vol", "recDate", "jalalidate",
                    "buy_I_Volume", "buy_I_Value", "buy_I_Count",
                    "buy_N_Volume", "buy_N_Value", "buy_N_Count",
                    "sell_I_Volume", "sell_I_Value", "sell_I_Count",
                    "sell_N_Volume", "sell_N_Value", "sell_N_Count",
                    "price_date_iso", "insCode",
                    "dollar", "ounces_gold", "thousand_dollar", "one_ounce"
                ]
                
                # اضافه کردن ستون‌های اضافی (به جز adjustment_ratio)
                extra_cols = [col for col in df.columns if col not in output_columns and col != 'adjustment_ratio']
                output_columns.extend(extra_cols)
                
                # اضافه کردن ستون adjustment_ratio در انتها
                if 'adjustment_ratio' in df.columns:
                    output_columns.append('adjustment_ratio')
                else:
                    # اگر ستون وجود ندارد، آن را ایجاد می‌کنیم
                    df['adjustment_ratio'] = 1.0
                    output_columns.append('adjustment_ratio')
                
                # اطمینان از وجود همه ستون‌ها
                for col in output_columns:
                    if col not in df.columns:
                        df[col] = None
                
                df = df[output_columns]
                
                # لاگ نتایج تعدیل
                if apply_adjustment and adjustments:
                    # اطمینان از وجود ستون adjustment_ratio
                    if 'adjustment_ratio' in df.columns:
                        adjusted_rows = df[df['adjustment_ratio'] != 1.0]
                        if not adjusted_rows.empty:
                            self.logger.info(
                                f"تعدیل برای {symbol}: {len(adjusted_rows)} ردیف از {len(df)} ردیف تعدیل شدند. "
                                f"ضریب‌های تعدیل: {adjusted_rows['adjustment_ratio'].unique()[:3]}"
                            )
                    else:
                        self.logger.warning(f"ستون adjustment_ratio برای {symbol} وجود ندارد")
                
                self.logger.info(f"دانلود {symbol} کامل شد: {len(df)} رکورد")
                return True, df
            else:
                self.logger.error(f"هیچ رکوردی برای {symbol} ایجاد نشد")
                return False, "هیچ رکوردی ایجاد نشد"
                
        except Exception as e:
            self.logger.error(f"خطا در دانلود داده {symbol}: {str(e)}", exc_info=True)
            return False, f"خطا: {str(e)}"

    def _create_adjustment_cache(self, adjustments: List[Dict]) -> Dict[str, Tuple[float, float]]:
        """ایجاد کش برای ضرایب تعدیل برای افزایش سرعت"""
        cache = {}
        
        # مرتب‌سازی تعدیل‌ها بر اساس تاریخ (جدید به قدیم)
        sorted_adjustments = sorted(adjustments, key=lambda x: x['dEven'], reverse=True)
        
        # محاسبه ضرایب تجمعی برای هر تاریخ تعدیل
        cumulative_price_ratio = 1.0
        cumulative_volume_ratio = 1.0
        
        for adj in sorted_adjustments:
            # برای تاریخ‌های بعد از این تعدیل، این ضرایب اعمال می‌شوند
            date_key = str(adj['dEven'])
            cache[date_key] = (cumulative_price_ratio, cumulative_volume_ratio)
            
            # به‌روزرسانی ضرایب تجمعی برای تاریخ‌های قبل‌تر
            cumulative_price_ratio *= adj['price_ratio']
            cumulative_volume_ratio *= adj['volume_ratio']
        
        return cache

    def _get_adjustment_ratios_for_date_cached(self, adjustments: List[Dict], target_date_str: str, 
                                             cache: Dict[str, Tuple[float, float]]) -> Tuple[float, float]:
        """دریافت ضرایب تعدیل برای تاریخ مشخص با استفاده از کش"""
        try:
            target_date = int(target_date_str)
            
            # اگر هیچ تعدیلی وجود ندارد
            if not adjustments:
                return 1.0, 1.0
            
            # یافتن اولین تعدیل که تاریخ آن بعد از تاریخ هدف باشد
            for adj in adjustments:
                if adj['dEven'] > target_date:
                    # استفاده از کش اگر موجود باشد
                    date_key = str(adj['dEven'])
                    if date_key in cache:
                        return cache[date_key]
                    
                    # محاسبه ضرایب تجمعی
                    price_ratio = 1.0
                    volume_ratio = 1.0
                    
                    # جمع‌آوری تمام تعدیل‌هایی که بعد از تاریخ هدف هستند
                    for a in adjustments:
                        if a['dEven'] > target_date:
                            price_ratio *= a['price_ratio']
                            volume_ratio *= a['volume_ratio']
                    
                    return price_ratio, volume_ratio
            
            # اگر هیچ تعدیلی بعد از تاریخ هدف نبود
            return 1.0, 1.0
            
        except Exception as e:
            self.logger.warning(f"خطا در محاسبه ضرایب تعدیل برای تاریخ {target_date_str}: {e}")
            return 1.0, 1.0

    def _parse_adjustment_data(self, adjustment_data: Dict) -> List[Dict]:
        """پردازش داده‌های تعدیل - منطق صحیح"""
        if not adjustment_data or 'instrumentShareChange' not in adjustment_data:
            return []
        
        adjustments = []
        for item in adjustment_data['instrumentShareChange']:
            dEven = item.get('dEven', 0)
            new_shares = float(item.get('numberOfShareNew', 1))
            old_shares = float(item.get('numberOfShareOld', 1))
            
            # اگر تعداد سهام تغییر نکرده باشد
            if new_shares == old_shares:
                continue
            
            # محاسبه ضرایب تعدیل صحیح:
            # قیمت‌ها: قدیم / جدید (برای کاهش قیمت قدیم)
            # حجم‌ها: جدید / قدیم (برای افزایش حجم قدیم)
            price_ratio = old_shares / new_shares  # < 1.0 (قیمت کاهش می‌یابد)
            volume_ratio = new_shares / old_shares  # > 1.0 (حجم افزایش می‌یابد)
            
            adjustment = {
                'dEven': dEven,
                'numberOfShareNew': new_shares,
                'numberOfShareOld': old_shares,
                'price_ratio': price_ratio,
                'volume_ratio': volume_ratio,
                'description': f"تغییر از {old_shares:,.0f} به {new_shares:,.0f} سهم (ضریب: {price_ratio:.6f}/{volume_ratio:.6f})"
            }
            adjustments.append(adjustment)
        
        # مرتب‌سازی بر اساس تاریخ (جدیدترین به قدیمی)
        adjustments.sort(key=lambda x: x['dEven'], reverse=True)
        
        return adjustments

    def _get_adjustment_ratios_for_date(self, adjustments: List[Dict], target_date_str: str) -> Tuple[float, float]:
        """دریافت ضرایب تعدیل برای تاریخ مشخص"""
        price_ratio = 1.0
        volume_ratio = 1.0
        
        if not adjustments:
            return price_ratio, volume_ratio
        
        try:
            target_date = int(target_date_str)
            
            # جمع‌آوری تمام تعدیل‌هایی که تاریخ آنها بعد از تاریخ هدف است
            # (افزایش سرمایه‌هایی که بعد از تاریخ معامله اتفاق افتاده‌اند)
            for adj in adjustments:
                if adj['dEven'] > target_date:
                    price_ratio *= adj['price_ratio']
                    volume_ratio *= adj['volume_ratio']
            
            return price_ratio, volume_ratio
            
        except Exception as e:
            self.logger.warning(f"خطا در محاسبه ضرایب تعدیل برای تاریخ {target_date_str}: {e}")
            return 1.0, 1.0

    def _apply_adjustment_to_record(self, record: Dict, price_ratio: float, volume_ratio: float) -> Dict:
        """اعمال تعدیل به یک رکورد - منطق صحیح"""
        if price_ratio == 1.0 and volume_ratio == 1.0:
            return record
        
        try:
            # 1. تعدیل قیمت‌ها: قیمت‌های قدیمی را کاهش می‌دهیم
            price_fields = ['pf', 'pl', 'pmin', 'pmax']
            for field in price_fields:
                if field in record and record[field] is not None and record[field] != '':
                    try:
                        original_value = float(record[field])
                        if original_value > 0:
                            adjusted_value = original_value * price_ratio
                            record[field] = int(adjusted_value)
                    except (ValueError, TypeError):
                        pass
            
            # 2. تعدیل حجم‌ها: حجم‌های قدیمی را افزایش می‌دهیم
            volume_fields = ['vol', 'buy_I_Volume', 'buy_N_Volume', 'sell_I_Volume', 'sell_N_Volume']
            for field in volume_fields:
                if field in record and record[field] is not None and record[field] != '':
                    try:
                        original_value = float(record[field])
                        if original_value > 0:
                            adjusted_value = original_value * volume_ratio
                            record[field] = int(adjusted_value)
                    except (ValueError, TypeError):
                        pass
            
            # 3. تعدیل ارزش‌ها: باید متناسب با تعدیل قیمت و حجم باشد
            # اما چون قیمت × حجم = ارزش، و ما قیمت را در ratio1 و حجم را در ratio2 ضرب کردیم،
            # پس ارزش باید در (ratio1 × ratio2) ضرب شود
            # اما ratio1 × ratio2 = (قدیم/جدید) × (جدید/قدیم) = 1.0
            # بنابراین ارزش نباید تغییر کند!
            # فقط برای اطمینان مجدداً محاسبه می‌کنیم
            
            # خرید حقیقی
            if record.get('buy_I_Volume', 0) > 0 and record.get('buy_I_Value', 0) > 0:
                # ارزش باید برابر با حجم × قیمت باشد (با استفاده از قیمت تعدیل شده)
                # اما از آنجایی که ارزش اصلی درست بوده، نیازی به تغییر نیست
                pass
            
            return record
            
        except Exception as e:
            self.logger.error(f"خطا در اعمال تعدیل به رکورد: {e}")
            return record

    
    def download_multiple_symbols(self, symbols_data: List[Tuple[str, str]], 
                                 progress_callback=None, apply_adjustment: bool = True) -> Dict[str, pd.DataFrame]:
        """دانلود چندین نماد به صورت موازی با پشتیبانی از تعدیل"""
        self.download_stats = {
            'total': len(symbols_data),
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'start_time': datetime.now(),
            'end_time': None,
            'apply_adjustment': apply_adjustment
        }
        
        results = {}
        failed_symbols = []
        
        self.logger.info(f"شروع دانلود {len(symbols_data)} نماد - حالت تعدیل: {apply_adjustment}")
        
        # استفاده از ThreadPoolExecutor برای دانلود موازی
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # ارسال همه کارها
            future_to_symbol = {
                executor.submit(self.download_symbol_data, symbol, internal_code, apply_adjustment): (symbol, internal_code)
                for symbol, internal_code in symbols_data
            }
            
            # پردازش نتایج
            for future in tqdm(concurrent.futures.as_completed(future_to_symbol), 
                             total=len(symbols_data), 
                             desc="دانلود نمادها"):
                symbol, internal_code = future_to_symbol[future]
                
                try:
                    success, result = future.result(timeout=60)
                    
                    if success and isinstance(result, pd.DataFrame) and not result.empty:
                        results[symbol] = result
                        self.download_stats['successful'] += 1
                        
                        if progress_callback:
                            progress_callback(symbol, True, f"دانلود {symbol} کامل شد (تعدیل: {apply_adjustment})")
                    else:
                        failed_symbols.append((symbol, result))
                        self.download_stats['failed'] += 1
                        
                        if progress_callback:
                            progress_callback(symbol, False, f"خطا در {symbol}: {result}")
                
                except concurrent.futures.TimeoutError:
                    failed_symbols.append((symbol, "Timeout"))
                    self.download_stats['failed'] += 1
                    
                    if progress_callback:
                        progress_callback(symbol, False, f"Timeout در {symbol}")
                
                except Exception as e:
                    failed_symbols.append((symbol, str(e)))
                    self.download_stats['failed'] += 1
                    
                    if progress_callback:
                        progress_callback(symbol, False, f"خطا در {symbol}: {str(e)}")
                
                # تاخیر بین درخواست‌ها
                time.sleep(self.delay_between_requests)
        
        self.download_stats['end_time'] = datetime.now()
        
        # لاگ نتایج
        self.logger.info(f"دانلود کامل شد: {self.download_stats['successful']} موفق، {self.download_stats['failed']} ناموفق، تعدیل: {apply_adjustment}")
        
        if failed_symbols:
            self.logger.warning(f"نمادهای ناموفق: {[s[0] for s in failed_symbols]}")
        
        return results
    
    def save_to_csv(self, df: pd.DataFrame, symbol: str, output_dir: str, 
                   add_timestamp: bool = False) -> Tuple[bool, str]:
        """ذخیره DataFrame به CSV با گزینه‌های مختلف"""
        try:
            # ایجاد پوشه خروجی
            os.makedirs(output_dir, exist_ok=True)
            
            # نام فایل
            filename = f"{symbol}.csv"
            if add_timestamp:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{symbol}_{timestamp}.csv"
            
            filepath = os.path.join(output_dir, filename)
            
            # تنظیمات ذخیره‌سازی
            save_kwargs = {
                'index': False,
                'encoding': 'utf-8-sig',  # UTF-8 با BOM برای سازگاری با Excel
                'date_format': '%Y-%m-%d' if 'recDate' in df.columns else None
            }
            
            # ذخیره فایل
            df.to_csv(filepath, **save_kwargs)
            
            # بررسی ذخیره‌سازی
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                self.logger.info(f"فایل {filename} ذخیره شد ({len(df)} رکورد، {os.path.getsize(filepath):,} بایت)")
                return True, filepath
            else:
                self.logger.error(f"فایل {filename} ذخیره نشد یا خالی است")
                return False, "فایل ذخیره نشد"
            
        except PermissionError:
            self.logger.error(f"خطای دسترسی در ذخیره {symbol}.csv")
            return False, "خطای دسترسی"
        except Exception as e:
            self.logger.error(f"خطا در ذخیره {symbol}.csv: {str(e)}")
            return False, f"خطا: {str(e)}"
    
    def save_to_excel(self, df: pd.DataFrame, symbol: str, output_dir: str) -> Tuple[bool, str]:
        """ذخیره DataFrame به Excel"""
        try:
            import pandas as pd
            os.makedirs(output_dir, exist_ok=True)
            
            filepath = os.path.join(output_dir, f"{symbol}.xlsx")
            
            # ایجاد writer Excel
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=symbol[:31], index=False)  # نام شیت حداکثر 31 کاراکتر
                
                # تنظیم عرض ستون‌ها
                worksheet = writer.sheets[symbol[:31]]
                for i, column in enumerate(df.columns, 1):
                    column_width = max(len(str(column)), df[column].astype(str).str.len().max())
                    worksheet.column_dimensions[chr(64 + i)].width = min(column_width + 2, 50)
            
            self.logger.info(f"فایل Excel {symbol}.xlsx ذخیره شد")
            return True, filepath
            
        except ImportError:
            self.logger.error("کتابخانه openpyxl برای ذخیره Excel نصب نیست")
            return False, "openpyxl نصب نیست"
        except Exception as e:
            self.logger.error(f"خطا در ذخیره Excel {symbol}: {str(e)}")
            return False, f"خطا: {str(e)}"
    
    def merge_dataframes(self, dataframes: Dict[str, pd.DataFrame], 
                        output_dir: str, filename: str = "merged_data") -> Tuple[bool, str]:
        """ادغام چند DataFrame در یک فایل"""
        try:
            if not dataframes:
                return False, "هیچ داده‌ای برای ادغام وجود ندارد"
            
            os.makedirs(output_dir, exist_ok=True)
            
            # روش 1: ذخیره در یک CSV با ستون نماد اضافه
            merged_records = []
            
            for symbol, df in dataframes.items():
                df_copy = df.copy()
                df_copy['symbol'] = symbol
                merged_records.append(df_copy)
            
            if merged_records:
                merged_df = pd.concat(merged_records, ignore_index=True)
                
                # ذخیره به CSV
                csv_path = os.path.join(output_dir, f"{filename}.csv")
                merged_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                
                self.logger.info(f"داده‌ها در {csv_path} ادغام شدند: {len(merged_df)} رکورد از {len(dataframes)} نماد")
                return True, csv_path
            
            return False, "هیچ رکوردی برای ادغام وجود ندارد"
            
        except Exception as e:
            self.logger.error(f"خطا در ادغام داده‌ها: {str(e)}")
            return False, f"خطا: {str(e)}"
    
    def compress_files(self, directory: str, output_filename: str = "data_archive") -> Tuple[bool, str]:
        """فشرده‌سازی فایل‌های CSV در یک ZIP"""
        try:
            import zipfile
            
            zip_path = os.path.join(directory, f"{output_filename}.zip")
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(directory):
                    for file in files:
                        if file.endswith('.csv'):
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, directory)
                            zipf.write(file_path, arcname)
            
            self.logger.info(f"فایل‌ها در {zip_path} فشرده شدند")
            return True, zip_path
            
        except ImportError:
            self.logger.error("ماژول zipfile در دسترس نیست")
            return False, "zipfile در دسترس نیست"
        except Exception as e:
            self.logger.error(f"خطا در فشرده‌سازی: {str(e)}")
            return False, f"خطا: {str(e)}"
    
    def get_download_stats(self) -> Dict:
        """دریافت آمار دانلود"""
        if self.download_stats['start_time'] and self.download_stats['end_time']:
            duration = self.download_stats['end_time'] - self.download_stats['start_time']
            self.download_stats['duration'] = str(duration)
        
        return self.download_stats
    
    def validate_internal_code(self, internal_code: str) -> bool:
        """اعتبارسنجی کد داخلی"""
        if not internal_code or not isinstance(internal_code, str):
            return False
        
        # حذف فاصله و کاراکترهای غیرعددی
        cleaned = ''.join(filter(str.isdigit, internal_code))
        
        # بررسی طول مناسب
        return 5 <= len(cleaned) <= 15
    
    def cleanup_cache(self, older_than_hours: int = 24):
        """پاکسازی فایل‌های کش قدیمی"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=older_than_hours)
            deleted_count = 0
            
            for filename in os.listdir(self.cache_dir):
                if filename.endswith('.json'):
                    filepath = os.path.join(self.cache_dir, filename)
                    file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                    
                    if file_time < cutoff_time:
                        os.remove(filepath)
                        deleted_count += 1
            
            if deleted_count > 0:
                self.logger.info(f"{deleted_count} فایل کش قدیمی پاک شد")
            
        except Exception as e:
            self.logger.error(f"خطا در پاکسازی کش: {str(e)}")
    
    def test_connection(self) -> Dict[str, bool]:
        """تست اتصال به APIها"""
        results = {}
        
        test_urls = {
            'data_url': self.config.settings.get("data_url", ""),
            'client_url': self.config.settings.get("client_url", "").format(inscode="123456"),
            'price_url': self.config.settings.get("price_url", "").format(inscode="123456"),
            'dollar_url': self.config.settings.get("dollar_url", ""),
            'gold_url': self.config.settings.get("gold_url", ""),
            'adjustment_url': f"https://cdn.tsetmc.com/api/Instrument/GetInstrumentShareChange/123456"
        }
        
        for name, url in test_urls.items():
            try:
                response = self._retry_request(url, max_retries=1, timeout=5)
                results[name] = response is not None and response.status_code == 200
                
                if results[name]:
                    self.logger.info(f"✅ {name}: قابل دسترس")
                else:
                    self.logger.warning(f"⚠️ {name}: غیرقابل دسترس")
                    
            except Exception as e:
                results[name] = False
                self.logger.error(f"❌ {name}: خطا - {str(e)}")
        
        return results

    def save_currency_files(self, output_dir: str) -> Tuple[bool, str]:
        """ذخیره فایل‌های دلار و طلا"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            messages = []
            
            # ذخیره فایل دلار
            if self.data_loader.dollar_data is not None and not self.data_loader.dollar_data.empty:
                dollar_df = self.data_loader.dollar_data.copy()
                
                # اضافه کردن ستون ticker و مرتب‌سازی
                dollar_df['ticker'] = 'دلار'
                
                # تغییر نام ستون‌ها به فرمت مورد نظر
                dollar_df = dollar_df.rename(columns={
                    'recDate': 'date',
                    'dollar_open': 'open',
                    'dollar_close': 'close',
                    'dollar_high': 'high',
                    'dollar_low': 'low',
                    'dollar_volume': 'volume'
                })
                
                # اطمینان از اینکه حجم مقدار دارد، اگر ندارد 0 قرار می‌دهیم
                dollar_df['volume'] = dollar_df['volume'].fillna(0).astype(int)
                
                # ترتیب ستون‌ها
                dollar_df = dollar_df[['ticker', 'date', 'open', 'close', 'high', 'low', 'volume']]
                
                # ذخیره فایل
                dollar_path = os.path.join(output_dir, 'dollar.csv')
                dollar_df.to_csv(dollar_path, index=False, encoding='utf-8-sig', date_format='%Y%m%d')
                messages.append(f'دلار: {len(dollar_df)} رکورد')
            
            # ذخیره فایل طلا
            if self.data_loader.gold_data is not None and not self.data_loader.gold_data.empty:
                gold_df = self.data_loader.gold_data.copy()
                
                # اضافه کردن ستون ticker و مرتب‌سازی
                gold_df['ticker'] = 'طلا'
                
                # تغییر نام ستون‌ها به فرمت مورد نظر
                gold_df = gold_df.rename(columns={
                    'recDate': 'date',
                    'gold_open': 'open',
                    'gold_close': 'close',
                    'gold_high': 'high',
                    'gold_low': 'low',
                    'gold_volume': 'volume'
                })
                
                # اطمینان از اینکه حجم مقدار دارد، اگر ندارد 0 قرار می‌دهیم
                gold_df['volume'] = gold_df['volume'].fillna(0).astype(int)
                
                # ترتیب ستون‌ها
                gold_df = gold_df[['ticker', 'date', 'open', 'close', 'high', 'low', 'volume']]
                
                # ذخیره فایل
                gold_path = os.path.join(output_dir, 'gold.csv')
                gold_df.to_csv(gold_path, index=False, encoding='utf-8-sig', date_format='%Y%m%d')
                messages.append(f'طلا: {len(gold_df)} رکورد')
            
            if messages:
                self.logger.info(f"فایل‌های ارز ذخیره شدند: {', '.join(messages)}")
                return True, 'فایل‌های دلار و طلا ذخیره شدند'
            else:
                self.logger.warning("هیچ داده‌ای برای ذخیره فایل‌های ارز وجود ندارد")
                return False, 'هیچ داده‌ای برای ذخیره وجود ندارد'
                
        except Exception as e:
            self.logger.error(f"خطا در ذخیره فایل‌های ارز: {str(e)}")
            return False, f"خطا در ذخیره فایل‌های ارز: {str(e)}"

# تابع کمکی برای استفاده خارجی
def create_downloader(config, data_loader):
    """ایجاد نمونه Downloader"""
    return Downloader(config, data_loader)

if __name__ == "__main__":
    # تست اجرای مستقل
    import sys
    sys.path.append('.')
    
    from config import Config
    from data_loader import DataLoader
    
    # راه‌اندازی لاگ
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-d %H:%M:%S'
    )
    
    config = Config()
    data_loader = DataLoader(config)
    
    # تست اتصال
    downloader = Downloader(config, data_loader)
    print("تست اتصال به APIها:")
    results = downloader.test_connection()
    for name, status in results.items():
        print(f"  {name}: {'✅' if status else '❌'}")
    
    # نمونه تست دانلود
    test_symbols = [
        ("شبندر", "35425587644315550"),
        ("فولاد", "46348559193224090")
    ]
    
    print(f"\nتست دانلود {len(test_symbols)} نماد...")
    
    for symbol, code in test_symbols:
        print(f"\nدانلود {symbol} با تعدیل...")
        success, result = downloader.download_symbol_data(symbol, code, apply_adjustment=True)
        
        if success and isinstance(result, pd.DataFrame):
            print(f"  موفق: {len(result)} رکورد دانلود شد")
            print(f"  ستون‌ها: {list(result.columns)[:15]}...")
            # نمایش ستون‌های جدید
            new_cols = ["dollar", "ounces_gold", "thousand_dollar", "one_ounce", "adjustment_ratio"]
            for col in new_cols:
                if col in result.columns:
                    sample_value = result[col].iloc[0] if len(result) > 0 else 'N/A'
                    print(f"  {col}: {sample_value}")
        else:
            print(f"  ناموفق: {result}")
    
    downloader.close_session()
    print("\nتست کامل شد.")