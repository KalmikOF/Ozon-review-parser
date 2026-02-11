"""
Ozon Review Parser
==============================================
Professional parser for Ozon marketplace reviews




Author: https://github.com/KalmikOF
"""

import json
import time
import os
import re
import threading
import queue
import random
import sys
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Selenium-wire для прокси с авторизацией
from seleniumwire import webdriver
from seleniumwire.utils import decode
from selenium.webdriver.chrome.options import Options


# ============================================
# КОНФИГУРАЦИЯ БРАУЗЕРОВ И ПРОКСИ
# ============================================
BROWSER_POOL_SIZE = 5  # Количество постоянно открытых браузеров

# Очистка куки после каждого товара
CLEAR_COOKIES_AFTER_PRODUCT = True  # True/False

# ============================================
# РЕЖИМЫ ПРОКСИ
# ============================================
# Доступные режимы:
# - "none"     : Без прокси (по умолчанию)
# - "single"   : Одна прокси на все браузеры
# - "rotation" : Ротация прокси через N товаров

PROXY_MODE = "none"  # ← Измени на "single" или "rotation"

# --- РЕЖИМ "single" (одна прокси на всех) ---
PROXY_SINGLE = "socks5://user:password@proxy.com:8080"

# --- РЕЖИМ "rotation" (смена прокси каждые N товаров) ---
PROXY_ROTATION_POOL = [
    "socks5://user1:pass1@proxy1.com:8080",
    "http://user2:pass2@proxy2.com:8080",
    "socks5://proxy3.com:1080",  # Без авторизации
    # ... добавь сколько нужно
]
ROTATION_INTERVAL = 5  # Менять прокси каждые N товаров
ROTATION_MODE = "random"  # "sequential" или "random"

# Счётчики для ротации (не трогай)
rotation_counters = {}
rotation_locks = {}


def get_proxy_for_browser(browser_id, products_parsed=0):
    """
    Возвращает прокси для браузера в зависимости от режима
    
    Args:
        browser_id: ID браузера (0-4)
        products_parsed: Количество спарсенных товаров (для ротации)
    
    Returns:
        str или None: прокси-строка или None
    """
    if PROXY_MODE == "none":
        return None
    
    elif PROXY_MODE == "single":
        return PROXY_SINGLE
    
    elif PROXY_MODE == "rotation":
        if not PROXY_ROTATION_POOL:
            return None
        
        # Инициализация счётчика для браузера
        if browser_id not in rotation_counters:
            rotation_counters[browser_id] = 0
            rotation_locks[browser_id] = threading.Lock()
        
        with rotation_locks[browser_id]:
            # Определяем индекс прокси
            interval_index = products_parsed // ROTATION_INTERVAL
            
            if ROTATION_MODE == "random":
                # Случайный выбор
                proxy_index = random.randint(0, len(PROXY_ROTATION_POOL) - 1)
            else:
                # Последовательный выбор
                proxy_index = interval_index % len(PROXY_ROTATION_POOL)
            
            return PROXY_ROTATION_POOL[proxy_index]
    
    return None


def setup_driver(profile_name="default", proxy=None):
    """Chrome с CDP, профилем и прокси через selenium-wire"""
    profile_dir = os.path.join(os.getcwd(), f"chrome_profile_ozon_{profile_name}")
    
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument(f"--user-data-dir={profile_dir}")
    
    # ============================================
    # АНТИ-ДЕТЕКТ И СТЕЛС
    # ============================================
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    
    # ============================================
    # БЛОКИРОВКА ГЕОЛОКАЦИИ И ПРИВАТНОСТЬ
    # ============================================
    # Блокировка геолокации
    chrome_options.add_experimental_option("prefs", {
        # Геолокация - БЛОКИРОВАТЬ
        "profile.default_content_setting_values.geolocation": 2,  # 1=разрешить, 2=блокировать
        
        # Уведомления - БЛОКИРОВАТЬ
        "profile.default_content_setting_values.notifications": 2,
        
        # Доступ к медиа (камера/микрофон) - БЛОКИРОВАТЬ
        "profile.default_content_setting_values.media_stream_mic": 2,
        "profile.default_content_setting_values.media_stream_camera": 2,
        
        # Всплывающие окна - БЛОКИРОВАТЬ
        "profile.default_content_setting_values.popups": 2,
        
        # Автозаполнение - ВЫКЛЮЧИТЬ
        "autofill.profile_enabled": False,
        "autofill.credit_card_enabled": False,
        
        # Сохранение паролей - ВЫКЛЮЧИТЬ
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        
        # Синхронизация - ВЫКЛЮЧИТЬ
        "sync.disabled": True,
        
        # Безопасный просмотр - МИНИМАЛЬНЫЙ (для скорости)
        "safebrowsing.enabled": False,
        
        # Предзагрузка страниц - ВЫКЛЮЧИТЬ (для скорости)
        "net.network_prediction_options": 2,
        
        # WebRTC - БЛОКИРОВАТЬ (утечка IP)
        "webrtc.ip_handling_policy": "disable_non_proxied_udp",
        "webrtc.multiple_routes_enabled": False,
        "webrtc.nonproxied_udp_enabled": False
    })
    
    # ============================================
    # ДОПОЛНИТЕЛЬНЫЕ АРГУМЕНТЫ ДЛЯ ПРИВАТНОСТИ
    # ============================================
    # Отключение WebRTC (утечка реального IP)
    chrome_options.add_argument("--disable-webrtc")
    chrome_options.add_argument("--disable-webrtc-ip-handling")
    
    # Отключение геолокации через аргументы
    chrome_options.add_argument("--disable-geolocation")
    
    # Отключение уведомлений
    chrome_options.add_argument("--disable-notifications")
    
    # Отключение синхронизации
    chrome_options.add_argument("--disable-sync")
    
    # Отключение GPU (для стабильности на серверах)
    chrome_options.add_argument("--disable-gpu")
    
    # Отключение dev-shm (для серверов с малым объёмом RAM)
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # ============================================
    # SSL И БЕЗОПАСНОСТЬ
    # ============================================
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--ignore-ssl-errors")
    chrome_options.add_argument("--allow-insecure-localhost")
    
    # ============================================
    # ПРОИЗВОДИТЕЛЬНОСТЬ
    # ============================================
    # Отключение изображений (опционально - для скорости)
    # chrome_options.add_experimental_option("prefs", {
    #     "profile.managed_default_content_settings.images": 2
    # })
    
    # ============================================
    # НАСТРОЙКА ПРОКСИ ЧЕРЕЗ SELENIUM-WIRE
    # ============================================
    seleniumwire_options = {}
    
    if proxy:
        print(f"[Setup] 🌐 Прокси: {proxy[:50]}...")
        
        # Парсим прокси
        if "://" in proxy:
            scheme, rest = proxy.split("://", 1)
        else:
            scheme = "http"
            rest = proxy
        
        # Парсим user:pass@host:port
        if "@" in rest:
            auth, host_port = rest.split("@", 1)
            if ":" in auth:
                user, password = auth.split(":", 1)
            else:
                user, password = auth, ""
        else:
            user, password = None, None
            host_port = rest
        
        # Парсим host:port
        if ":" in host_port:
            host, port = host_port.rsplit(":", 1)
        else:
            host = host_port
            port = "8080"
        
        # Формируем прокси для selenium-wire
        if scheme == "socks5":
            if user and password:
                proxy_url = f"socks5://{user}:{password}@{host}:{port}"
            else:
                proxy_url = f"socks5://{host}:{port}"
        else:
            if user and password:
                proxy_url = f"http://{user}:{password}@{host}:{port}"
            else:
                proxy_url = f"http://{host}:{port}"
        
        # Настройка selenium-wire
        seleniumwire_options = {
            'proxy': {
                'http': proxy_url,
                'https': proxy_url,
                'no_proxy': 'localhost,127.0.0.1'
            },
            'verify_ssl': False,
            'suppress_connection_errors': True
        }
        
        if user and password:
            print(f"[Setup] 🔐 Авторизация: {user}:***")
    
    # ============================================
    # СОЗДАНИЕ ДРАЙВЕРА
    # ============================================
    driver = webdriver.Chrome(
        options=chrome_options,
        seleniumwire_options=seleniumwire_options
    )
    
    # ============================================
    # ФИНАЛЬНЫЕ НАСТРОЙКИ ЧЕРЕЗ JS
    # ============================================
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    # Подмена геолокации (если она всё же запрашивается)
    driver.execute_cdp_cmd("Emulation.setGeolocationOverride", {
        "latitude": 0,
        "longitude": 0,
        "accuracy": 100
    })
    
    # Подмена timezone (опционально - можно настроить под прокси)
    # driver.execute_cdp_cmd("Emulation.setTimezoneOverride", {
    #     "timezoneId": "Europe/Moscow"
    # })
    
    # Проверка IP если есть прокси
    if proxy:
        print(f"[Setup] ⏳ Проверка прокси...")
        try:
            driver.get("http://api.ipify.org?format=json")
            time.sleep(2)
            ip_info = driver.find_element("tag name", "pre").text
            print(f"[Setup] ✅ IP: {ip_info}")
            
            import json
            current_ip = json.loads(ip_info)['ip']
            if current_ip == host:
                print(f"[Setup] 🎉 ПРОКСИ РАБОТАЕТ!")
            else:
                print(f"[Setup] ⚠️ IP: {current_ip} (ожидался {host})")
        except Exception as e:
            print(f"[Setup] ⚠️ Проверка IP: {e}")
    
    print(f"[Setup] ✅ Профиль настроен (геолокация, WebRTC, уведомления - БЛОКИРОВАНЫ)")
    
    return driver
    driver = None
    profile_name = f"pool_{worker_id}"
    
    print(f"[Браузер {worker_id}] 🚀 Запуск...")
    
    while True:
        # Берём URL из очереди
        try:
            url = url_queue.get(timeout=1)
        except queue.Empty:
            # Очередь пуста - выходим
            break
        
        try:
            # Если браузер не открыт - открываем
            if driver is None:
                try:
                    driver = setup_driver(profile_name)
                    print(f"[Браузер {worker_id}] ✅ Профиль {profile_name} открыт")
                except Exception as e:
                    print(f"[Браузер {worker_id}] ❌ Не удалось открыть браузер: {e}")
                    url_queue.task_done()
                    continue
            
            # Парсим товар
            print(f"\n[Браузер {worker_id}] 🔗 Парсинг: {url}")
            
            driver.get(url)
            time.sleep(3)
            
            product_name = get_product_name(driver)
            print(f"[Браузер {worker_id}] 📦 Товар: {product_name}")
            
            try_click_reviews_tab(driver)
            time.sleep(2)
            
            if not try_open_first_review(driver):
                print(f"[Браузер {worker_id}] ⚠️ Нет отзывов")
                url_queue.task_done()
                continue
            
            time.sleep(2)
            
            # Парсинг отзывов
            reviews_data = []
            seen_uuids = set()
            max_reviews = 600
            
            while len(reviews_data) < max_reviews:
                time.sleep(1.5)
                
                review = parse_active_review_adaptive(driver)
                
                if not review or not review.get('found'):
                    print(f"[Браузер {worker_id}]    ❌ Парсинг не удался")
                    break
                
                uuid = review['review_uuid']
                
                if uuid not in seen_uuids:
                    seen_uuids.add(uuid)
                    reviews_data.append(review)
                    
                    if len(reviews_data) % 10 == 0:
                        print(f"[Браузер {worker_id}]    ✅ Собрано: {len(reviews_data)}")
                
                if not navigate_to_next_review(driver, uuid, max_clicks=50):
                    print(f"[Браузер {worker_id}]    ℹ️  Конец списка")
                    break
            
            print(f"\n[Браузер {worker_id}] ✅ ЗАВЕРШЁН! Собрано: {len(reviews_data)}")
            
            finalize_media(reviews_data)
            
            total_videos = sum(len(r["videos"]) for r in reviews_data)
            total_images = sum(len(r["images"]) for r in reviews_data)
            
            print(f"[Браузер {worker_id}]    📹 Видео: {total_videos}")
            print(f"[Браузер {worker_id}]    🖼️  Фото: {total_images}")
            
            # Сохранение
            if reviews_data:
                safe_name = re.sub(r'[\\/*?:"<>|]', '_', product_name)
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                json_filename = f"{safe_name}_{timestamp}.json"
                json_path = os.path.join(results_dir, json_filename)
                
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(reviews_data, f, ensure_ascii=False, indent=2)
                
                print(f"[Браузер {worker_id}] 💾 Сохранено: {json_path}")
                
                results_list.append({
                    'success': True,
                    'product_name': product_name,
                    'reviews_count': len(reviews_data),
                    'json_path': json_path
                })
            
        except Exception as e:
            print(f"[Браузер {worker_id}] ❌ ОШИБКА: {e}")
            
            # При ошибке - перезапускаем браузер
            if driver:
                try:
                    driver.quit()
                except:
                    pass
                driver = None
                print(f"[Браузер {worker_id}] 🔄 Браузер закрыт, будет перезапущен")
            
            results_list.append({
                'success': False,
                'product_name': 'unknown',
                'error': str(e)
            })
        
        finally:
            url_queue.task_done()
    
    # Закрываем браузер при выходе
    if driver:
        try:
            driver.quit()
            print(f"[Браузер {worker_id}] 👋 Закрыт")
        except:
            pass


def get_profile_id(worker_id):
    """Возвращает ID профиля из пула (0 до BROWSER_POOL_SIZE-1)"""
    return worker_id % BROWSER_POOL_SIZE


def worker_thread(worker_id, url_queue, results_list, results_dir):
    """
    Один постоянный браузер, который обрабатывает задачи из очереди
    
    ЛОГИКА:
    1. Открывает браузер с профилем pool_{worker_id} и прокси
    2. Берёт URL из очереди
    3. Парсит товар
    4. Очищает куки (если CLEAR_COOKIES_AFTER_PRODUCT = True)
    5. При ошибке → перезапускает браузер (со сменой прокси если rotation)
    """
    driver = None
    profile_name = f"pool_{worker_id}"
    products_parsed = 0  # Счётчик для ротации прокси
    
    print(f"[Браузер {worker_id}] 🚀 Запуск...")
    
    while True:
        # Берём URL из очереди
        try:
            url = url_queue.get(timeout=1)
        except queue.Empty:
            break
        
        try:
            # Если браузер не открыт - открываем с прокси
            if driver is None:
                try:
                    proxy = get_proxy_for_browser(worker_id, products_parsed)
                    driver = setup_driver(profile_name, proxy)
                    print(f"[Браузер {worker_id}] ✅ Профиль {profile_name} открыт")
                except Exception as e:
                    print(f"[Браузер {worker_id}] ❌ setup_driver: {e}")
                    url_queue.task_done()
                    continue
            
            # Парсим товар
            print(f"\n[Браузер {worker_id}] 🔗 {url}")
            
            driver.get(url)
            time.sleep(3)
            
            product_name = get_product_name(driver)
            print(f"[Браузер {worker_id}] 📦 {product_name}")
            
            try_click_reviews_tab(driver)
            time.sleep(2)
            
            if not try_open_first_review(driver):
                print(f"[Браузер {worker_id}] ⚠️ Нет отзывов")
                
                # Очистка куки
                if CLEAR_COOKIES_AFTER_PRODUCT:
                    driver.delete_all_cookies()
                    print(f"[Браузер {worker_id}] 🧹 Куки очищены")
                
                # task_done() будет вызван в finally
                continue
            
            time.sleep(2)
            
            # Парсинг отзывов
            reviews_data = []
            seen_uuids = set()
            max_reviews = 600
            
            while len(reviews_data) < max_reviews:
                time.sleep(1.5)
                
                review = parse_active_review_adaptive(driver)
                
                if not review or not review.get('found'):
                    break
                
                uuid = review['review_uuid']
                
                if uuid not in seen_uuids:
                    seen_uuids.add(uuid)
                    reviews_data.append(review)
                    
                    if len(reviews_data) % 10 == 0:
                        print(f"[Браузер {worker_id}]    ✅ Собрано: {len(reviews_data)}")
                
                if not navigate_to_next_review(driver, uuid, max_clicks=50):
                    break
            
            print(f"\n[Браузер {worker_id}] ✅ Собрано: {len(reviews_data)}")
            
            finalize_media(reviews_data)
            
            # Сохранение
            if reviews_data:
                safe_name = re.sub(r'[\\/*?:"<>|]', '_', product_name)
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                json_filename = f"{safe_name}_{timestamp}.json"
                json_path = os.path.join(results_dir, json_filename)
                
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(reviews_data, f, ensure_ascii=False, indent=2)
                
                print(f"[Браузер {worker_id}] 💾 {json_path}")
                
                results_list.append({
                    'success': True,
                    'product_name': product_name,
                    'reviews_count': len(reviews_data),
                    'json_path': json_path
                })
            
            # Очистка куки после успешного парсинга
            if CLEAR_COOKIES_AFTER_PRODUCT:
                driver.delete_all_cookies()
                print(f"[Браузер {worker_id}] 🧹 Куки очищены")
            
            products_parsed += 1
            
        except Exception as e:
            print(f"[Браузер {worker_id}] ❌ {e}")
            
            # При ошибке - перезапускаем браузер
            if driver:
                try:
                    driver.quit()
                except:
                    pass
                driver = None
                print(f"[Браузер {worker_id}] 🔄 Перезапуск...")
            
            results_list.append({
                'success': False,
                'product_name': 'unknown',
                'error': str(e)
            })
        
        finally:
            url_queue.task_done()
    
    # Закрываем браузер при выходе
    if driver:
        try:
            driver.quit()
            print(f"[Браузер {worker_id}] 👋 Закрыт")
        except:
            pass


def get_product_name(driver):
    """Извлекает название товара"""
    script = """
    let selectors = [
        'h1',
        '[data-widget="webProductHeading"] h1',
        '.tsHeadline500Medium',
        '[class*="ProductTitle"]'
    ];
    
    for (let selector of selectors) {
        let element = document.querySelector(selector);
        if (element && element.textContent.trim()) {
            return element.textContent.trim();
        }
    }
    
    return document.title.split('—')[0].trim();
    """
    
    try:
        product_name = driver.execute_script(script)
        if product_name:
            product_name = re.sub(r'[<>:"/\\|?*]', '_', product_name)
            if len(product_name) > 100:
                product_name = product_name[:100]
            return product_name
    except:
        pass
    
    return "unknown_product"


def try_click_reviews_tab(driver):
    """Кликает на вкладку отзывов"""
    script = """
    let tabs = Array.from(document.querySelectorAll('a, button, div[role="tab"]'));
    
    for (let tab of tabs) {
        let text = (tab.textContent || '').toLowerCase();
        if (text.includes('отзыв') || text.includes('фото') || text.includes('видео')) {
            tab.click();
            return true;
        }
    }
    return false;
    """
    try:
        return driver.execute_script(script)
    except:
        return False


def try_open_first_review(driver):
    """Открывает первый отзыв в модалке"""
    script = """
    // Ищем кликабельные элементы с фото/видео
    let buttons = document.querySelectorAll('button, a, div[role="button"]');
    
    for (let btn of buttons) {
        let img = btn.querySelector('img[src*="cover"], img[src*="photo"], img[src*="video"]');
        if (img) {
            btn.click();
            return true;
        }
    }
    
    // Если не нашли - ищем любые медиа
    let mediaElements = document.querySelectorAll('img[src*="ozon"], video');
    if (mediaElements.length > 0) {
        let parent = mediaElements[0].closest('button, a, div[role="button"]');
        if (parent) {
            parent.click();
            return true;
        }
    }
    
    return false;
    """
    try:
        return driver.execute_script(script)
    except:
        return False


def parse_active_review_adaptive(driver):
    """
    АДАПТИВНЫЙ ПАРСИНГ v5.0
    =======================
    ✅ Семантический анализ структуры
    ✅ Не зависит от точных CSS-классов
    ✅ Обрабатывает 2 или 3 span структуры
    ✅ Основан на паттернах, а не на классах
    """
    script = """
    let allReviews = document.querySelectorAll('[data-review-uuid]');
    
    if (allReviews.length === 0) {
        return {found: false, error: 'Нет [data-review-uuid]'};
    }
    
    // Находим активный отзыв (правый)
    let review = allReviews[allReviews.length - 1];
    let rect = review.getBoundingClientRect();
    
    if (rect.left < 900) {
        for (let r of allReviews) {
            let rRect = r.getBoundingClientRect();
            if (rRect.left > 900) {
                review = r;
                break;
            }
        }
    }
    
    let data = {
        found: true,
        review_uuid: review.getAttribute('data-review-uuid') || '',
        author: '',
        date: '',
        text: '',
        rating: 0,
        media_items: [],
        media_buttons_count: 0
    };
    
    // ==========================================
    // АВТОР - Семантический подход
    // ==========================================
    let allSpans = review.querySelectorAll('span');
    let authorCandidates = [];
    
    // Метод 1: Класс содержит "kr" + короткий текст
    for (let span of allSpans) {
        let className = span.className || '';
        let text = (span.textContent || '').trim();
        
        if (className.match(/\\bkr\\w*_?\\d+/) && text.length > 2 && text.length < 100 && !text.includes('\\n')) {
            authorCandidates.push(text);
        }
    }
    
    // Метод 2: Собираем все короткие span (включая первую букву)
    if (authorCandidates.length === 0) {
        for (let span of allSpans) {
            let text = (span.textContent || '').trim();
            if (text.length >= 1 && text.length < 50 && !text.includes('\\n')) {
                authorCandidates.push(text);
                if (authorCandidates.join('').length > 10) break; // Достаточно
            }
        }
    }
    
    // Склеиваем кандидатов
    if (authorCandidates.length > 0) {
        data.author = authorCandidates.join('');
    }
    
    // ==========================================
    // ДАТА - Regex (стабильный)
    // ==========================================
    let fullText = review.textContent;
    let dateMatch = fullText.match(/(\\d{1,2}\\s+(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\\s+\\d{4})/);
    if (dateMatch) {
        data.date = dateMatch[1];
    }
    
    // ==========================================
    // ТЕКСТ - Семантический подход
    // ==========================================
    
    // Метод 1: Класс содержит "ku" или "kt" + длинный текст
    for (let span of allSpans) {
        let className = span.className || '';
        let text = (span.textContent || '').trim();
        
        if (className.match(/\\b(ku|kt)\\w*_?\\d+/) && text.length > 20) {
            data.text = text;
            break;
        }
    }
    
    // Метод 2: Самый длинный span
    if (!data.text) {
        let maxLength = 0;
        let longestText = '';
        
        for (let span of allSpans) {
            let text = (span.textContent || '').trim();
            if (text.length > maxLength && text.length > 20) {
                maxLength = text.length;
                longestText = text;
            }
        }
        
        if (longestText) {
            data.text = longestText;
        }
    }
    
    // ==========================================
    // РЕЙТИНГ - Звёзды badge (находятся ВНЕ review!)
    // ==========================================
    // КРИТИЧНО: Звёзды badge находятся в DOCUMENT, не внутри review!
    // Ищем звёзды в верхней части экрана (top: 60-100px)
    
    let allSvgs = document.querySelectorAll('svg');  // ← DOCUMENT, не review!
    let badgeStars = [];
    
    // Ищем звёзды в верхней части (где badge)
    for (let svg of allSvgs) {
        let rect = svg.getBoundingClientRect();
        
        // Звёзды badge: top 60-100px, размер 15-25px
        if (rect.top >= 60 && rect.top <= 100 && 
            rect.width >= 15 && rect.width <= 25 && 
            rect.height >= 15 && rect.height <= 25) {
            
            let style = window.getComputedStyle(svg);
            let color = style.color;
            
            // Оранжевая или серая звезда
            if (color.includes('255, 165, 0') || color.includes('0, 26, 52')) {
                badgeStars.push({
                    svg: svg,
                    left: rect.left,
                    top: rect.top,
                    isFilled: color.includes('255, 165, 0')
                });
            }
        }
    }
    
    // Сортируем по left (горизонтальная линия)
    badgeStars.sort(function(a, b) {
        return a.left - b.left;
    });
    
    // Находим группу из 5 звёзд подряд на одной линии
    let rating = 0;
    if (badgeStars.length >= 5) {
        for (let i = 0; i <= badgeStars.length - 5; i++) {
            let group = badgeStars.slice(i, i + 5);
            
            // Проверяем, что 5 звёзд на одной линии (top разница < 5px)
            let minTop = Math.min(...group.map(function(s) { return s.top; }));
            let maxTop = Math.max(...group.map(function(s) { return s.top; }));
            
            // И расстояние между звёздами одинаковое (~20px)
            let leftDiffs = [];
            for (let j = 1; j < group.length; j++) {
                leftDiffs.push(group[j].left - group[j-1].left);
            }
            let avgDiff = leftDiffs.reduce(function(a, b) { return a + b; }, 0) / leftDiffs.length;
            
            // Условия: на одной линии И равные промежутки (18-22px)
            if (maxTop - minTop < 5 && avgDiff >= 18 && avgDiff <= 22) {
                // Это группа рейтинга badge!
                for (let j = 0; j < group.length; j++) {
                    if (group[j].isFilled) {
                        rating++;
                    }
                }
                break;
            }
        }
    }
    
    data.rating = rating;
    
    // ==========================================
    // МЕДИА - Стабильные селекторы
    // ==========================================
    let mediaButtons = review.querySelectorAll('button img[src*="/cover/"], button img[src*="rp-photo"], button img[src*="/video-"]');
    data.media_buttons_count = mediaButtons.length;
    
    let seenUUIDs = new Set();
    
    // ВИДЕО
    let videoImgs = review.querySelectorAll('img[src*="/video-"]');
    videoImgs.forEach(function(img) {
        let src = img.src;
        let match = src.match(/\\/video-(\\d+)\\/([A-Z0-9]+)\\//);
        if (match) {
            let serverNum = match[1];
            let uuid = match[2];
            if (!seenUUIDs.has(uuid)) {
                seenUUIDs.add(uuid);
                data.media_items.push({
                    type: 'video',
                    uuid: uuid,
                    server_num: serverNum,
                    url: `https://vr-1.ozone.ru/sashimi/video-${serverNum}/${uuid}/asset_1_h264.mp4`
                });
            }
        }
    });
    
    // ФОТО - rp-photo
    let photoImgs = review.querySelectorAll('img[src*="/rp-photo-"]');
    photoImgs.forEach(function(img) {
        let src = img.src;
        let match = src.match(/\\/rp-photo-(\\d+)\\/wc\\d+\\/([a-f0-9\\-]+)\\.(jpg|jpeg|png)/);
        if (match) {
            let serverNum = match[1];
            let uuid = match[2];
            let ext = match[3];
            if (!seenUUIDs.has(uuid)) {
                seenUUIDs.add(uuid);
                data.media_items.push({
                    type: 'photo',
                    uuid: uuid,
                    server_num: serverNum,
                    url_1000: `https://ir.ozone.ru/s3/rp-photo-${serverNum}/wc1200/${uuid}.${ext}`,
                    url_400: `https://ir.ozone.ru/s3/rp-photo-${serverNum}/wc400/${uuid}.${ext}`
                });
            }
        }
    });
    
    // ФОТО - cover
    let coverImgs = review.querySelectorAll('img[src*="/cover/"]');
    coverImgs.forEach(function(img) {
        let src = img.src;
        let match = src.match(/\\/cover\\/(\\d+)\\/([a-f0-9\\-]+)\\.(jpg|jpeg|png)/);
        if (match) {
            let serverNum = match[1];
            let uuid = match[2];
            let ext = match[3];
            if (!seenUUIDs.has(uuid)) {
                seenUUIDs.add(uuid);
                data.media_items.push({
                    type: 'photo',
                    uuid: uuid,
                    server_num: serverNum,
                    url_cover: `https://ir.ozone.ru/s3/multimedia-w/cover/${serverNum}/${uuid}.${ext}`
                });
            }
        }
    });
    
    return data;
    """
    
    try:
        return driver.execute_script(script)
    except Exception as e:
        return {"found": False, "error": str(e)}


def finalize_media(reviews_data):
    """Финализация медиа"""
    for review in reviews_data:
        media_items = review.get('media_items', [])
        
        videos = []
        images = []
        
        for item in media_items:
            if item['type'] == 'video':
                videos.append(item['url'])
            elif item['type'] == 'photo':
                if 'url_1000' in item:
                    images.append(item['url_1000'])
                elif 'url_400' in item:
                    images.append(item['url_400'])
                elif 'url_cover' in item:
                    images.append(item['url_cover'])
        
        review['videos'] = videos
        review['images'] = images
        
        review.pop('media_items', None)
        review.pop('media_buttons_count', None)


def click_next(driver):
    """Кликает Next - ОРИГИНАЛЬНАЯ ЛОГИКА из v3"""
    script = """
    let buttons = Array.from(document.querySelectorAll('button'));
    
    // Фильтруем только ВИДИМЫЕ кнопки
    buttons = buttons.filter(btn => {
        let style = window.getComputedStyle(btn);
        return style.display !== 'none' && style.visibility !== 'hidden' && btn.offsetParent !== null;
    });
    
    // Ищем по aria-label
    let nextBtn = buttons.find(btn => {
        let label = btn.getAttribute('aria-label') || '';
        return label.toLowerCase().includes('next') || 
               label.toLowerCase().includes('след') ||
               label === 'Next slide';
    });
    
    if (nextBtn) {
        nextBtn.click();
        return true;
    }
    
    // Fallback: ищем кнопки с SVG стрелками
    let svgButtons = buttons.filter(btn => {
        let svg = btn.querySelector('svg');
        if (!svg) return false;
        let svgHtml = svg.innerHTML.toLowerCase();
        return svgHtml.includes('arrow') || svgHtml.includes('right') || svgHtml.includes('chevron');
    });
    
    if (svgButtons.length > 0) {
        svgButtons[svgButtons.length - 1].click();
        return true;
    }
    
    return false;
    """
    
    try:
        return driver.execute_script(script)
    except:
        return False


def navigate_to_next_review(driver, current_uuid, max_clicks=50):
    """
    НАВИГАЦИЯ v5.1 - Проверенная логика из v3
    ==========================================
    Кликает "Далее" до 50 раз, проверяя UUID после каждого клика
    Останавливается когда UUID сменится
    
    Returns:
        True - UUID сменился (новый отзыв)
        False - Не удалось сменить UUID после max_clicks кликов
    """
    clicks_count = 0
    
    while clicks_count < max_clicks:
        if not click_next(driver):
            return False  # Нет кнопки далее
        
        clicks_count += 1
        time.sleep(1.5)
        
        # Проверяем UUID после каждого клика
        new_review = parse_active_review_adaptive(driver)
        
        if new_review and new_review.get('found'):
            new_uuid = new_review['review_uuid']
            
            if new_uuid != current_uuid:
                # ✅ UUID сменился - успех!
                time.sleep(2.0)  # Даем время на полную загрузку
                return True
        else:
            # Не удалось спарсить - возможно конец
            return False
    
    # После 50 кликов UUID не сменился - конец списка
    return False


def read_urls_from_file(txt_path):
    """Читает ссылки из txt файла"""
    urls = []
    
    try:
        with open(txt_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and 'ozon.ru' in line:
                    urls.append(line)
    except Exception as e:
        print(f"❌ Ошибка чтения файла: {e}")
    
    return urls


def parse_single_product(product_url, worker_id, output_dir):
    """Парсинг одного товара"""
    driver = None
    
    try:
        print(f"\n[Воркер {worker_id}] 🚀 Старт: {product_url}")
        
        # Используем профиль из пула (0-4)
        profile_id = get_profile_id(worker_id)
        driver = setup_driver(f"pool_{profile_id}")
        driver.get(product_url)
        time.sleep(3)
        
        product_name = get_product_name(driver)
        print(f"[Воркер {worker_id}] 📦 Товар: {product_name}")
        
        try_click_reviews_tab(driver)
        time.sleep(2)
        
        # Кликаем на первую кнопку медиа
        first_button = driver.execute_script("""
            let buttons = document.querySelectorAll('button img[src*="/cover/"], button img[src*="rp-photo"], button img[src*="/video-"]');
            if (buttons.length > 0) {
                return buttons[0].closest('button');
            }
            return null;
        """)
        
        if not first_button:
            print(f"[Воркер {worker_id}] ❌ Нет медиа-кнопок")
            return {
                "url": product_url,
                "success": False,
                "error": "Нет медиа-кнопок"
            }
        
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", first_button)
            time.sleep(1)
            first_button.click()
        except:
            driver.execute_script("arguments[0].click();", first_button)
        
        time.sleep(4)
        
        # Проверяем модалку
        modal_check = driver.execute_script("""
            return document.querySelector('[data-review-uuid]') !== null;
        """)
        
        if not modal_check:
            print(f"[Воркер {worker_id}] ⚠️  Модалка не открылась")
            return {
                "url": product_url,
                "success": False,
                "error": "Модалка не открылась"
            }
        
        print(f"[Воркер {worker_id}] ✅ Модалка открыта!")
        
        # Тест парсинга
        print(f"[Воркер {worker_id}] 🧪 Тест...")
        test_review = parse_active_review_adaptive(driver)
        
        if not test_review.get('found'):
            print(f"[Воркер {worker_id}] ❌ Ошибка: {test_review.get('error')}")
            return {
                "url": product_url,
                "success": False,
                "error": test_review.get('error')
            }
        
        print(f"[Воркер {worker_id}]    ✅ UUID: {test_review['review_uuid'][:8]}...")
        print(f"[Воркер {worker_id}]    ✅ Автор: {test_review.get('author', 'НЕТ')}")
        print(f"[Воркер {worker_id}]    ✅ Дата: {test_review.get('date', 'НЕТ')}")
        print(f"[Воркер {worker_id}]    ✅ Текст: {test_review.get('text', 'НЕТ')[:30]}...")
        
        print(f"\n[Воркер {worker_id}] 🔄 Парсинг...\n")
        
        reviews_data = []
        seen_uuids = set()
        
        max_reviews = 600
        
        while len(reviews_data) < max_reviews:
            time.sleep(1.5)
            
            review = parse_active_review_adaptive(driver)
            
            if not review or not review.get('found'):
                print(f"[Воркер {worker_id}]    ❌ Парсинг не удался")
                break
            
            uuid = review['review_uuid']
            
            # Проверяем, новый ли это отзыв
            if uuid not in seen_uuids:
                # ✅ Новый отзыв - сохраняем
                seen_uuids.add(uuid)
                reviews_data.append(review)
                
                if len(reviews_data) % 10 == 0:
                    print(f"[Воркер {worker_id}]    ✅ Собрано: {len(reviews_data)}")
            # Дубликаты просто игнорируем (это медиа карусели)
            
            # НАВИГАЦИЯ - пытаемся найти следующий отзыв
            # Функция кликает до 50 раз пока UUID не сменится
            if not navigate_to_next_review(driver, uuid, max_clicks=50):
                # 50 кликов и UUID не сменился = реальный конец списка
                print(f"[Воркер {worker_id}]    ℹ️  Конец списка (50 кликов без смены UUID)")
                break
        
        print(f"\n[Воркер {worker_id}] ✅ ЗАВЕРШЁН! Собрано: {len(reviews_data)}")
        
        finalize_media(reviews_data)
        
        total_videos = sum(len(r["videos"]) for r in reviews_data)
        total_images = sum(len(r["images"]) for r in reviews_data)
        
        print(f"[Воркер {worker_id}]    📹 Видео: {total_videos}")
        print(f"[Воркер {worker_id}]    🖼️  Фото: {total_images}")
        
        result = {
            "product_url": product_url.split("?")[0],
            "product_name": product_name,
            "parsed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_reviews": len(reviews_data),
            "total_videos": total_videos,
            "total_images": total_images,
            "reviews": reviews_data
        }
        
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"{product_name}_{timestamp}.json")
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"[Воркер {worker_id}] 💾 Сохранено: {output_file}")
        
        return {
            "url": product_url,
            "product_name": product_name,
            "reviews_count": len(reviews_data),
            "output_file": output_file,
            "success": True
        }
        
    except Exception as e:
        print(f"[Воркер {worker_id}] ❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return {
            "url": product_url,
            "success": False,
            "error": str(e)
        }
        
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


def main():
    print("="*80)
    print("  OZON PARSER v6.1 - PROXY & COOKIES MANAGEMENT")
    print("  ✅ ⭐ РЕЙТИНГ РАБОТАЕТ!")
    print("  ✅ ПУЛ ИЗ 5 ПОСТОЯННЫХ БРАУЗЕРОВ")
    print("  ✅ 🆕 ОЧИСТКА КУКИ после каждого товара")
    print("  ✅ 🆕 ПРОКСИ: HTTP/SOCKS5 + авторизация (selenium-wire)")
    print("  ✅ 🆕 АНТИ-ДЕТЕКТ: геолокация, WebRTC - БЛОКИРОВАНЫ")
    print("="*80)
    print(f"\n📦 Количество браузеров: {BROWSER_POOL_SIZE}")
    print(f"🧹 Очистка куки: {'ВКЛ' if CLEAR_COOKIES_AFTER_PRODUCT else 'ВЫКЛ'}")
    print(f"🌐 Режим прокси: {PROXY_MODE.upper()}")
    
    if PROXY_MODE != "none":
        if PROXY_MODE == "single":
            print(f"   Прокси: {PROXY_SINGLE[:50]}...")
        elif PROXY_MODE == "rotation":
            print(f"   Пул прокси: {len(PROXY_ROTATION_POOL)} штук")
            print(f"   Смена каждые: {ROTATION_INTERVAL} товаров ({ROTATION_MODE})")
    print()
    
    txt_path = input("📄 Введите путь к txt файлу со ссылками: ").strip()
    
    if not os.path.exists(txt_path):
        print(f"❌ Файл не найден: {txt_path}")
        return
    
    urls = read_urls_from_file(txt_path)
    
    if not urls:
        print("❌ В файле нет ссылок!")
        return
    
    print(f"✅ Найдено ссылок: {len(urls)}")
    for i, url in enumerate(urls, 1):
        print(f"   {i}. {url}")
    
    output_dir = os.path.dirname(txt_path)
    results_dir = os.path.join(output_dir, "results")
    os.makedirs(results_dir, exist_ok=True)
    
    print(f"\n📁 Результаты: {results_dir}")
    
    # Создаём очередь задач
    url_queue = queue.Queue()
    for url in urls:
        url_queue.put(url)
    
    # Список для результатов (thread-safe)
    results_list = []
    
    print(f"\n🚀 Запускаю {BROWSER_POOL_SIZE} постоянных браузеров...")
    print("="*80)
    
    # Создаём пул потоков-браузеров
    threads = []
    for i in range(BROWSER_POOL_SIZE):
        t = threading.Thread(
            target=worker_thread,
            args=(i, url_queue, results_list, results_dir),
            daemon=True
        )
        t.start()
        threads.append(t)
    
    # Ждём завершения всех задач
    try:
        url_queue.join()
    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем! Завершаю работу...")
    
    # Ждём завершения всех потоков
    for t in threads:
        t.join(timeout=5)
    
    print("\n" + "="*80)
    print("📊 ИТОГОВАЯ СТАТИСТИКА")
    print("="*80)
    
    successful = [r for r in results_list if r.get('success')]
    failed = [r for r in results_list if not r.get('success')]
    
    print(f"\n✅ Успешно: {len(successful)} из {len(urls)}")
    print(f"❌ Ошибок: {len(failed)}")
    
    if successful:
        print("\n📦 УСПЕШНЫЕ ТОВАРЫ:")
        for i, r in enumerate(successful, 1):
            print(f"   {i}. {r['product_name']} - {r['reviews_count']} отзывов")
    
    if failed:
        print("\n❌ ОШИБКИ:")
        for i, r in enumerate(failed, 1):
            print(f"   {i}. {r.get('product_name', 'unknown')} - {r.get('error', 'unknown error')}")
    
    print("\n" + "="*80)
    print("✅ ПАРСИНГ ЗАВЕРШЁН!")
    print("="*80)


if __name__ == "__main__":
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Ozon Parser v6.1')
    parser.add_argument('--config', type=str, help='Путь к конфиг-файлу из GUI')
    args = parser.parse_args()
    
    # Если передан конфиг-файл - загружаем настройки из него
    if args.config and os.path.exists(args.config):
        print("="*80)
        print("📄 ЗАГРУЗКА КОНФИГУРАЦИИ ИЗ GUI")
        print("="*80)
        
        with open(args.config, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # Применяем настройки из конфига
        BROWSER_POOL_SIZE = config.get('browser_count', 5)
        CLEAR_COOKIES_AFTER_PRODUCT = config.get('clear_cookies', True)
        PROXY_MODE = config.get('proxy_mode', 'none')
        PROXY_SINGLE = config.get('proxy_single', '')
        PROXY_ROTATION_POOL = config.get('proxy_list', [])
        ROTATION_INTERVAL = config.get('rotation_interval', 5)
        ROTATION_MODE = config.get('rotation_mode', 'random')
        
        # Обновляем глобальные переменные
        import __main__
        __main__.BROWSER_POOL_SIZE = BROWSER_POOL_SIZE
        __main__.CLEAR_COOKIES_AFTER_PRODUCT = CLEAR_COOKIES_AFTER_PRODUCT
        __main__.PROXY_MODE = PROXY_MODE
        __main__.PROXY_SINGLE = PROXY_SINGLE
        __main__.PROXY_ROTATION_POOL = PROXY_ROTATION_POOL
        __main__.ROTATION_INTERVAL = ROTATION_INTERVAL
        __main__.ROTATION_MODE = ROTATION_MODE
        
        print(f"✅ Конфигурация загружена:")
        print(f"   Браузеров: {BROWSER_POOL_SIZE}")
        print(f"   Очистка куки: {CLEAR_COOKIES_AFTER_PRODUCT}")
        print(f"   Прокси: {PROXY_MODE}")
        print("="*80)
        print()
        
        # Запускаем main() с конфигом
        urls_file = config.get('urls_file')
        if urls_file and os.path.exists(urls_file):
            # Автоматический запуск без input
            urls = read_urls_from_file(urls_file)
            if urls:
                output_dir = os.path.dirname(urls_file)
                results_dir = os.path.join(output_dir, "results")
                os.makedirs(results_dir, exist_ok=True)
                
                print(f"✅ Найдено ссылок: {len(urls)}")
                print(f"📁 Результаты: {results_dir}")
                print()
                
                # Создаём очередь задач
                url_queue = queue.Queue()
                for url in urls:
                    url_queue.put(url)
                
                results_list = []
                
                print(f"🚀 Запускаю {BROWSER_POOL_SIZE} постоянных браузеров...")
                print("="*80)
                
                # Создаём пул потоков-браузеров
                threads = []
                for i in range(BROWSER_POOL_SIZE):
                    t = threading.Thread(
                        target=worker_thread,
                        args=(i, url_queue, results_list, results_dir),
                        daemon=True
                    )
                    t.start()
                    threads.append(t)
                
                # Ждём завершения всех задач
                try:
                    url_queue.join()
                except KeyboardInterrupt:
                    print("\n\n⚠️  Прервано пользователем! Завершаю работу...")
                
                # Ждём завершения всех потоков
                for t in threads:
                    t.join(timeout=5)
                
                print("\n" + "="*80)
                print("📊 ИТОГОВАЯ СТАТИСТИКА")
                print("="*80)
                
                successful = [r for r in results_list if r.get('success')]
                failed = [r for r in results_list if not r.get('success')]
                
                print(f"\n✅ Успешно: {len(successful)} из {len(urls)}")
                print(f"❌ Ошибок: {len(failed)}")
                
                if successful:
                    print("\n📦 УСПЕШНЫЕ ТОВАРЫ:")
                    for i, r in enumerate(successful, 1):
                        print(f"   {i}. {r['product_name']} - {r['reviews_count']} отзывов")
                
                if failed:
                    print("\n❌ ОШИБКИ:")
                    for i, r in enumerate(failed, 1):
                        print(f"   {i}. {r.get('product_name', 'unknown')} - {r.get('error', 'unknown error')}")
                
                print("\n" + "="*80)
                print("✅ ПАРСИНГ ЗАВЕРШЁН!")
                print("="*80)
            else:
                print("❌ В файле нет ссылок!")
        else:
            print("❌ Файл со ссылками не найден!")
    else:
        # Обычный режим - интерактивный
        main()