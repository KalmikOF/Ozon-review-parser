"""
Ozon Review Parser - GUI
=========================
Graphical user interface for Ozon review parser

Author: https://github.com/KalmikOF
Repository: https://github.com/KalmikOF/ozon-review-parser
"""

import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
import threading
import queue as queue_module
import os
import json
import time
import sys
import re

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import parser module
try:
    import ozon_parser as parser_module
    
    # Import necessary functions
    setup_driver = parser_module.setup_driver
    get_product_name = parser_module.get_product_name
    try_click_reviews_tab = parser_module.try_click_reviews_tab
    try_open_first_review = parser_module.try_open_first_review
    parse_active_review_adaptive = parser_module.parse_active_review_adaptive
    navigate_to_next_review = parser_module.navigate_to_next_review
    finalize_media = parser_module.finalize_media
    
    PARSER_LOADED = True
except ImportError as e:
    print(f"⚠️ Failed to import parser: {e}")
    print("📄 Make sure ozon_parser.py is in the same directory!")
    PARSER_LOADED = False


class OzonParserGUI:
    """Main GUI application class"""
    
    def __init__(self, root):
        """Initialize GUI application"""
        self.root = root
        self.root.title("Ozon Review Parser")
        self.root.geometry("1000x750")
        self.root.resizable(True, True)
        
        # Check if parser loaded
        if not PARSER_LOADED:
            messagebox.showerror(
                "Error",
                "Failed to load parser module!\n\n"
                "Make sure ozon_parser.py is in the same directory."
            )
            self.root.destroy()
            return
        
        # Interface variables
        self.urls_file = tk.StringVar()
        self.browser_count = tk.IntVar(value=5)
        self.clear_cookies = tk.BooleanVar(value=True)
        self.proxy_mode = tk.StringVar(value="none")
        self.rotation_interval = tk.IntVar(value=5)
        self.rotation_mode = tk.StringVar(value="random")
        self.proxy_single = tk.StringVar(value="socks5://user:pass@proxy.com:8080")
        
        # Parser variables
        self.proxy_list = []
        self.is_running = False
        self.parser_threads = []
        self.url_queue = queue_module.Queue()
        self.results_list = []
        self.total_urls = 0
        self.completed_urls = 0
        
        # Rotation counters
        self.rotation_counters = {}
        self.rotation_locks = {}
        
        self.setup_ui()
        
        # GUI update queue
        self.gui_queue = queue_module.Queue()
        self.root.after(100, self.process_gui_queue)
    
    def setup_ui(self):
        """Create user interface"""
        
        # Header
        header_frame = tk.Frame(self.root, bg="#2c3e50", height=60)
        header_frame.pack(fill=tk.X)
        header_frame.pack_propagate(False)
        
        title_label = tk.Label(
            header_frame,
            text="🛒 Ozon Review Parser",
            font=("Arial", 18, "bold"),
            bg="#2c3e50",
            fg="white"
        )
        title_label.pack(pady=15)
        
        # Main container
        main_container = tk.Frame(self.root, padx=20, pady=20)
        main_container.pack(fill=tk.BOTH, expand=True)
        
        # File selection section
        file_frame = tk.LabelFrame(
            main_container,
            text="📄 URLs File",
            font=("Arial", 10, "bold"),
            padx=10,
            pady=10
        )
        file_frame.pack(fill=tk.X, pady=(0, 10))
        
        file_entry_frame = tk.Frame(file_frame)
        file_entry_frame.pack(fill=tk.X)
        
        self.file_entry = tk.Entry(
            file_entry_frame,
            textvariable=self.urls_file,
            font=("Arial", 10)
        )
        self.file_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        
        browse_btn = tk.Button(
            file_entry_frame,
            text="📁 Browse",
            command=self.browse_file,
            bg="#3498db",
            fg="white",
            font=("Arial", 9, "bold"),
            cursor="hand2",
            padx=15
        )
        browse_btn.pack(side=tk.RIGHT)
        
        self.file_info_label = tk.Label(
            file_frame,
            text="No file selected",
            font=("Arial", 9),
            fg="gray"
        )
        self.file_info_label.pack(anchor=tk.W, pady=(5, 0))
        
        # Settings section
        settings_frame = tk.LabelFrame(
            main_container,
            text="⚙️ Settings",
            font=("Arial", 10, "bold"),
            padx=10,
            pady=10
        )
        settings_frame.pack(fill=tk.X, pady=(0, 10))
        
        row1 = tk.Frame(settings_frame)
        row1.pack(fill=tk.X, pady=5)
        
        tk.Label(row1, text="Browsers:", font=("Arial", 9)).pack(side=tk.LEFT, padx=(0, 10))
        tk.Spinbox(
            row1,
            from_=1,
            to=10,
            textvariable=self.browser_count,
            width=5,
            font=("Arial", 9)
        ).pack(side=tk.LEFT, padx=(0, 30))
        
        tk.Checkbutton(
            row1,
            text="Clear cookies after each product",
            variable=self.clear_cookies,
            font=("Arial", 9)
        ).pack(side=tk.LEFT)
        
        # Proxy section
        proxy_frame = tk.LabelFrame(
            main_container,
            text="🌐 Proxy Settings",
            font=("Arial", 10, "bold"),
            padx=10,
            pady=10
        )
        proxy_frame.pack(fill=tk.X, pady=(0, 10))
        
        tk.Radiobutton(
            proxy_frame,
            text="No proxy",
            variable=self.proxy_mode,
            value="none",
            font=("Arial", 9),
            command=self.update_proxy_fields
        ).pack(anchor=tk.W)
        
        tk.Radiobutton(
            proxy_frame,
            text="Single proxy",
            variable=self.proxy_mode,
            value="single",
            font=("Arial", 9),
            command=self.update_proxy_fields
        ).pack(anchor=tk.W)
        
        self.single_proxy_frame = tk.Frame(proxy_frame)
        self.single_proxy_frame.pack(fill=tk.X, padx=(20, 0), pady=(5, 10))
        
        tk.Label(self.single_proxy_frame, text="Proxy:", font=("Arial", 9)).pack(
            side=tk.LEFT,
            padx=(0, 10)
        )
        tk.Entry(
            self.single_proxy_frame,
            textvariable=self.proxy_single,
            font=("Arial", 9),
            width=50
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        tk.Radiobutton(
            proxy_frame,
            text="Proxy rotation",
            variable=self.proxy_mode,
            value="rotation",
            font=("Arial", 9),
            command=self.update_proxy_fields
        ).pack(anchor=tk.W)
        
        self.rotation_frame = tk.Frame(proxy_frame)
        self.rotation_frame.pack(fill=tk.X, padx=(20, 0), pady=(5, 0))
        
        rotation_settings = tk.Frame(self.rotation_frame)
        rotation_settings.pack(fill=tk.X, pady=(0, 5))
        
        tk.Label(rotation_settings, text="Change every:", font=("Arial", 9)).pack(
            side=tk.LEFT,
            padx=(0, 5)
        )
        tk.Spinbox(
            rotation_settings,
            from_=1,
            to=100,
            textvariable=self.rotation_interval,
            width=5,
            font=("Arial", 9)
        ).pack(side=tk.LEFT, padx=(0, 10))
        tk.Label(rotation_settings, text="products", font=("Arial", 9)).pack(
            side=tk.LEFT,
            padx=(0, 20)
        )
        
        tk.Label(rotation_settings, text="Mode:", font=("Arial", 9)).pack(
            side=tk.LEFT,
            padx=(0, 5)
        )
        ttk.Combobox(
            rotation_settings,
            textvariable=self.rotation_mode,
            values=["random", "sequential"],
            state="readonly",
            width=12,
            font=("Arial", 9)
        ).pack(side=tk.LEFT)
        
        load_proxy_btn = tk.Button(
            self.rotation_frame,
            text="📝 Load proxies from file",
            command=self.load_proxy_file,
            bg="#27ae60",
            fg="white",
            font=("Arial", 9, "bold"),
            cursor="hand2",
            padx=10
        )
        load_proxy_btn.pack(anchor=tk.W, pady=(0, 5))
        
        self.proxy_list_frame = tk.Frame(self.rotation_frame)
        self.proxy_list_frame.pack(fill=tk.BOTH, expand=True)
        
        proxy_scroll = tk.Scrollbar(self.proxy_list_frame)
        proxy_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.proxy_listbox = tk.Listbox(
            self.proxy_list_frame,
            height=3,
            font=("Consolas", 8),
            yscrollcommand=proxy_scroll.set
        )
        self.proxy_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        proxy_scroll.config(command=self.proxy_listbox.yview)
        
        self.update_proxy_fields()
        
        # Log section
        log_frame = tk.LabelFrame(
            main_container,
            text="📋 Execution Log",
            font=("Arial", 10, "bold"),
            padx=10,
            pady=10
        )
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            height=8,
            font=("Consolas", 9),
            wrap=tk.WORD,
            bg="#f8f9fa"
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Progress section
        progress_frame = tk.Frame(main_container)
        progress_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.progress_label = tk.Label(
            progress_frame,
            text="Ready to start",
            font=("Arial", 9, "bold")
        )
        self.progress_label.pack(anchor=tk.W, pady=(0, 5))
        
        self.progress_bar = ttk.Progressbar(progress_frame, mode='determinate')
        self.progress_bar.pack(fill=tk.X)
        
        # Control buttons
        buttons_frame = tk.Frame(main_container)
        buttons_frame.pack(fill=tk.X)
        
        self.start_btn = tk.Button(
            buttons_frame,
            text="🚀 START PARSING",
            command=self.start_parsing,
            bg="#e74c3c",
            fg="white",
            font=("Arial", 12, "bold"),
            cursor="hand2",
            height=2,
            width=20
        )
        self.start_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        self.stop_btn = tk.Button(
            buttons_frame,
            text="⏹ STOP",
            command=self.stop_parsing,
            bg="#95a5a6",
            fg="white",
            font=("Arial", 10, "bold"),
            cursor="hand2",
            state=tk.DISABLED,
            width=15
        )
        self.stop_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        self.results_btn = tk.Button(
            buttons_frame,
            text="📂 OPEN RESULTS",
            command=self.open_results,
            bg="#3498db",
            fg="white",
            font=("Arial", 10, "bold"),
            cursor="hand2",
            width=20
        )
        self.results_btn.pack(side=tk.LEFT)
    
    def update_proxy_fields(self):
        """Update proxy fields visibility based on selected mode"""
        mode = self.proxy_mode.get()
        
        if mode == "single":
            self.single_proxy_frame.pack(fill=tk.X, padx=(20, 0), pady=(5, 10))
            self.rotation_frame.pack_forget()
        elif mode == "rotation":
            self.single_proxy_frame.pack_forget()
            self.rotation_frame.pack(fill=tk.X, padx=(20, 0), pady=(5, 0))
        else:
            self.single_proxy_frame.pack_forget()
            self.rotation_frame.pack_forget()
    
    def browse_file(self):
        """Browse for URLs file"""
        filename = filedialog.askopenfilename(
            title="Select URLs file",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if filename:
            self.urls_file.set(filename)
            
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    urls = [line.strip() for line in f if line.strip()]
                self.file_info_label.config(
                    text=f"✅ Found {len(urls)} URLs",
                    fg="green"
                )
            except Exception as e:
                self.file_info_label.config(
                    text=f"❌ Error reading file: {e}",
                    fg="red"
                )
    
    def load_proxy_file(self):
        """Load proxies from file"""
        filename = filedialog.askopenfilename(
            title="Select proxy file",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if filename:
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    proxies = [line.strip() for line in f if line.strip()]
                
                self.proxy_list = proxies
                self.proxy_listbox.delete(0, tk.END)
                
                for proxy in proxies:
                    display_proxy = proxy
                    if "@" in proxy and ":" in proxy.split("@")[0]:
                        parts = proxy.split("@")
                        auth = parts[0].split("://")[-1]
                        if ":" in auth:
                            user = auth.split(":")[0]
                            display_proxy = proxy.replace(auth, f"{user}:***")
                    
                    self.proxy_listbox.insert(tk.END, display_proxy)
                
                self.gui_log(f"✅ Loaded {len(proxies)} proxies")
            except Exception as e:
                self.gui_log(f"❌ Error loading proxies: {e}")
                messagebox.showerror("Error", f"Failed to load proxies:\n{e}")
    
    def gui_log(self, message):
        """Add message to log (thread-safe)"""
        self.gui_queue.put(('log', message))
    
    def process_gui_queue(self):
        """Process GUI update queue"""
        try:
            while True:
                action, data = self.gui_queue.get_nowait()
                
                if action == 'log':
                    self.log_text.insert(tk.END, data + "\n")
                    self.log_text.see(tk.END)
                elif action == 'progress':
                    self.completed_urls = data
                    if self.total_urls > 0:
                        progress = (self.completed_urls / self.total_urls) * 100
                        self.progress_bar['value'] = progress
                        self.progress_label.config(
                            text=f"Progress: {self.completed_urls}/{self.total_urls} ({progress:.1f}%)"
                        )
                elif action == 'complete':
                    self.parsing_complete()
                    
        except queue_module.Empty:
            pass
        
        self.root.after(100, self.process_gui_queue)
    
    def get_proxy_for_browser(self, browser_id, products_parsed=0):
        """Get proxy for browser based on mode"""
        mode = self.proxy_mode.get()
        
        if mode == "none":
            return None
        elif mode == "single":
            return self.proxy_single.get()
        elif mode == "rotation":
            if not self.proxy_list:
                return None
            
            if browser_id not in self.rotation_counters:
                self.rotation_counters[browser_id] = 0
                self.rotation_locks[browser_id] = threading.Lock()
            
            with self.rotation_locks[browser_id]:
                import random
                interval_index = products_parsed // self.rotation_interval.get()
                
                if self.rotation_mode.get() == "random":
                    proxy_index = random.randint(0, len(self.proxy_list) - 1)
                else:
                    proxy_index = interval_index % len(self.proxy_list)
                
                return self.proxy_list[proxy_index]
        
        return None
    
    def worker_thread(self, worker_id, results_dir):
        """Worker thread - manages one browser"""
        driver = None
        profile_name = f"browser_{worker_id}"
        products_parsed = 0
        
        self.gui_log(f"[Browser {worker_id}] 🚀 Starting...")
        
        while self.is_running:
            try:
                url = self.url_queue.get(timeout=1)
            except queue_module.Empty:
                break
            
            try:
                if driver is None:
                    proxy = self.get_proxy_for_browser(worker_id, products_parsed)
                    driver = setup_driver(profile_name, proxy)
                    self.gui_log(f"[Browser {worker_id}] ✅ Ready")
                
                self.gui_log(f"[Browser {worker_id}] 🔗 {url[:60]}...")
                
                driver.get(url)
                time.sleep(3)
                
                product_name = get_product_name(driver)
                self.gui_log(f"[Browser {worker_id}] 📦 {product_name}")
                
                try_click_reviews_tab(driver)
                time.sleep(2)
                
                if not try_open_first_review(driver):
                    self.gui_log(f"[Browser {worker_id}] ⚠️ No reviews")
                    
                    if self.clear_cookies.get():
                        driver.delete_all_cookies()
                    
                    # Add to failed results
                    self.results_list.append({
                        'success': False,
                        'product_name': product_name,
                        'error': 'No reviews found'
                    })
                    
                    self.url_queue.task_done()
                    self.gui_queue.put(('progress', self.completed_urls + 1))
                    continue
                
                time.sleep(2)
                
                # Parse reviews
                reviews_data = []
                seen_uuids = set()
                max_reviews = 600
                
                while len(reviews_data) < max_reviews and self.is_running:
                    time.sleep(1.5)
                    
                    review = parse_active_review_adaptive(driver)
                    
                    if not review or not review.get('found'):
                        break
                    
                    uuid = review['review_uuid']
                    
                    if uuid not in seen_uuids:
                        seen_uuids.add(uuid)
                        reviews_data.append(review)
                        
                        if len(reviews_data) % 10 == 0:
                            self.gui_log(f"[Browser {worker_id}]    ✅ Collected: {len(reviews_data)}")
                    
                    if not navigate_to_next_review(driver, uuid, max_clicks=50):
                        break
                
                self.gui_log(f"[Browser {worker_id}] ✅ COMPLETED! Collected: {len(reviews_data)}")
                
                finalize_media(reviews_data)
                
                # Save results
                if reviews_data:
                    safe_name = re.sub(r'[\\/*?:"<>|]', '_', product_name)
                    timestamp = time.strftime("%Y%m%d_%H%M%S")
                    json_filename = f"{safe_name}_{timestamp}.json"
                    json_path = os.path.join(results_dir, json_filename)
                    
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(reviews_data, f, ensure_ascii=False, indent=2)
                    
                    self.gui_log(f"[Browser {worker_id}] 💾 Saved")
                    
                    self.results_list.append({
                        'success': True,
                        'product_name': product_name,
                        'reviews_count': len(reviews_data),
                        'json_path': json_path
                    })
                
                if self.clear_cookies.get():
                    driver.delete_all_cookies()
                
                products_parsed += 1
                
            except Exception as e:
                self.gui_log(f"[Browser {worker_id}] ❌ {str(e)[:100]}")
                
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
                    driver = None
                
                self.results_list.append({
                    'success': False,
                    'product_name': 'unknown',
                    'error': str(e)
                })
            
            finally:
                self.url_queue.task_done()
                self.gui_queue.put(('progress', self.completed_urls + 1))
        
        if driver:
            try:
                driver.quit()
                self.gui_log(f"[Browser {worker_id}] 👋 Closed")
            except:
                pass
    
    def start_parsing(self):
        """Start parsing process"""
        if not self.urls_file.get():
            messagebox.showwarning("Warning", "Select URLs file!")
            return
        
        if not os.path.exists(self.urls_file.get()):
            messagebox.showerror("Error", "File not found!")
            return
        
        if self.proxy_mode.get() == "rotation" and not self.proxy_list:
            messagebox.showwarning("Warning", "Load proxies for rotation mode!")
            return
        
        # Read URLs
        try:
            with open(self.urls_file.get(), 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f if line.strip()]
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read file:\n{e}")
            return
        
        if not urls:
            messagebox.showwarning("Warning", "No URLs found in file!")
            return
        
        # Initialize
        self.total_urls = len(urls)
        self.completed_urls = 0
        self.results_list = []
        self.is_running = True
        
        # Clear queue
        while not self.url_queue.empty():
            try:
                self.url_queue.get_nowait()
            except:
                break
        
        # Add URLs
        for url in urls:
            self.url_queue.put(url)
        
        # Create results directory
        results_dir = os.path.join(os.path.dirname(self.urls_file.get()), "results")
        os.makedirs(results_dir, exist_ok=True)
        
        # Logging
        self.log_text.delete(1.0, tk.END)
        self.gui_log("="*60)
        self.gui_log("🚀 STARTING PARSER")
        self.gui_log("="*60)
        self.gui_log(f"📄 URLs: {self.total_urls}")
        self.gui_log(f"📦 Browsers: {self.browser_count.get()}")
        self.gui_log(f"🧹 Clear cookies: {'Yes' if self.clear_cookies.get() else 'No'}")
        self.gui_log(f"🌐 Proxy: {self.proxy_mode.get()}")
        
        if self.proxy_mode.get() == "rotation":
            self.gui_log(f"   Proxy pool: {len(self.proxy_list)}")
        self.gui_log("")
        
        # Update interface
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.progress_bar['value'] = 0
        self.progress_label.config(text=f"Progress: 0/{self.total_urls} (0.0%)")
        
        # Start threads
        self.parser_threads = []
        for i in range(self.browser_count.get()):
            t = threading.Thread(
                target=self.worker_thread,
                args=(i, results_dir),
                daemon=True
            )
            t.start()
            self.parser_threads.append(t)
        
        # Monitor completion
        threading.Thread(target=self.monitor_completion, daemon=True).start()
    
    def monitor_completion(self):
        """Monitor parsing completion"""
        for t in self.parser_threads:
            t.join()
        
        if self.is_running:
            self.gui_queue.put(('complete', None))
    
    def parsing_complete(self):
        """Handle parsing completion"""
        self.is_running = False
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        
        successful = [r for r in self.results_list if r.get('success')]
        failed = [r for r in self.results_list if not r.get('success')]
        
        self.gui_log("")
        self.gui_log("="*60)
        self.gui_log("📊 FINAL STATISTICS")
        self.gui_log("="*60)
        self.gui_log(f"✅ Successful: {len(successful)} of {self.total_urls}")
        self.gui_log(f"❌ Failed: {len(failed)}")
        self.gui_log("="*60)
        
        self.progress_label.config(text=f"✅ Completed: {len(successful)}/{self.total_urls}")
        
        messagebox.showinfo(
            "Completed",
            f"Parsing completed!\n\nSuccessful: {len(successful)}\nFailed: {len(failed)}"
        )
    
    def stop_parsing(self):
        """Stop parsing"""
        self.is_running = False
        self.gui_log("\n⏹ Stopping parser...")
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.progress_label.config(text="Stopped")
    
    def open_results(self):
        """Open results directory"""
        if self.urls_file.get():
            results_dir = os.path.join(os.path.dirname(self.urls_file.get()), "results")
            if os.path.exists(results_dir):
                os.startfile(results_dir)
            else:
                messagebox.showinfo("Info", "Results directory not created yet")
        else:
            messagebox.showwarning("Warning", "Select URLs file first")


def main():
    """Main entry point"""
    root = tk.Tk()
    app = OzonParserGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()