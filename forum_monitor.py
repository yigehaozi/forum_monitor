import time
import xml.etree.ElementTree as ET
import requests
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
import os
import re
import json
import shutil
from threading import Thread, Event, Lock
from collections import defaultdict
from plugins.plugin import Plugin
import threading

class Forum_monitor(Plugin):
    """
    论坛新帖监控插件
    监控WordPress网站的sitemap,发现新帖时推送通知
    基础命令：
    - TS帮助: 显示命令菜单
    - TS测试: 测试监控和推送功能
    - TS开启: 开启推送
    - TS关闭: 关闭推送
    - TS清理: 清除已处理的URL缓存
    - TS状态: 查看当前状态
    - TS间隔 <秒数>: 设置检查间隔时间
    - TS推送 <URL>: 再次推送指定URL的帖子
    
    高级功能：
    - TS忽略旧帖: 忽略当前时间之前的帖子
    - TS历史记录: 导出历史推送记录
    
    备份功能：
    - TS备份: 手动备份数据
    - TS备份设置 开启/关闭: 开启或关闭自动备份
    - TS备份间隔 <小时>: 设置备份间隔
    - TS备份数量 <数量>: 设置保留的备份数量
    
    重试机制：
    - TS重试 开启/关闭: 开启或关闭失败重试
    - TS重试次数 <次数>: 设置最大重试次数
    - TS重试间隔 <秒数>: 设置重试间隔
    
    频率限制：
    - TS频率 开启/关闭: 开启或关闭推送频率限制
    - TS频率设置 <次数/分钟>: 设置每分钟最大推送次数
    
    时间段设置：
    - TS时段 开启/关闭: 开启或关闭时间段限制
    - TS时段设置 <开始时间> <结束时间>: 设置推送时间段(格式:HH:MM)
    
    内容过滤：
    - TS过滤 开启/关闭: 开启或关闭内容过滤
    - TS过滤词 添加/删除 <关键词>: 管理过滤关键词
    - TS过滤词列表: 查看所有过滤关键词
    
    数据源管理：
    - TS源 添加 <名称> <URL>: 添加新的sitemap源
    - TS源 删除 <名称>: 删除指定sitemap源
    - TS源 列表: 查看所有sitemap源
    - TS源 开启/关闭 <名称>: 启用或禁用指定源
    
    推送模板：
    - TS模板 添加 <名称> <模板内容>: 添加新的推送模板
    - TS模板 删除 <名称>: 删除指定模板
    - TS模板 列表: 查看所有模板
    - TS模板 设置 <名称>: 设置当前使用的模板
    
    分组管理：
    - TS分组 创建 <名称>: 创建新的推送分组
    - TS分组 删除 <名称>: 删除指定分组
    - TS分组 添加 <分组> <群ID/用户ID>: 添加推送对象到分组
    - TS分组 移除 <分组> <群ID/用户ID>: 从分组移除推送对象
    - TS分组 列表: 查看所有分组
    
    历史记录管理：
    - TS历史清理 开启/关闭: 开启或关闭自动清理
    - TS历史天数 <天数>: 设置保留天数
    - TS历史立即清理: 立即清理过期记录
    """
    
    name = 'Forum_monitor'
    _data_file = os.path.join(os.path.dirname(__file__), 'data.json')
    _backup_dir = os.path.join(os.path.dirname(__file__), 'backups')
    _rate_limit_lock = Lock()
    _processing_lock = Lock()
    _check_lock = Lock()  # 添加检查锁
    _last_push_time = 0
    _push_count = 0
    _push_reset_time = 0
    _retry_queue = []
    _processing_urls = set()  # 存储正在处理的URL
    
    def __init__(self, wcf, msg):
        super().__init__(wcf, msg)
        self.last_check_time = 0
        self._stop_event = Event()
        self._monitor_thread = None
        self._backup_thread = None
        self._retry_thread = None
        self._cleanup_thread = None
        self._processed_urls = set()  # 初始化为空集合
        self._history = []  # 初始化为空列表
        self._load_data()  # 加载数据
        os.makedirs(self._backup_dir, exist_ok=True)
        self._start_background_tasks()

    def _start_background_tasks(self):
        """启动后台任务"""
        try:
            # 启动备份线程
            if self.data['settings']['backup']['enabled']:
                self._start_backup_thread()
            
            # 启动重试线程
            if self.data['settings']['retry']['enabled']:
                self._start_retry_thread()
                
            # 启动清理线程
            if self.data['settings']['history_cleanup']['enabled']:
                self._start_cleanup_thread()
        except Exception as e:
            print(f"启动后台任务失败: {e}")

    def _load_data(self):
        """加载持久化数据"""
        try:
            if os.path.exists(self._data_file):
                with open(self._data_file, 'r', encoding='utf-8') as f:
                    self.data = json.load(f)
                    # 从data中加载已处理的URLs
                    self._processed_urls = set(self.data.get('processed_urls', []))
                    # 从历史记录中添加非processing状态的URL
                    self._processed_urls.update(
                        record['url'] for record in self.data.get('history', [])
                        if record.get('status') not in ['processing', None]
                    )
                    self._history = self.data.get('history', [])
                    settings = self.data.get('settings', {})
                    self._is_running = settings.get('is_running', False)
                    self._ignore_old = settings.get('ignore_old', False)
                    # 确保monitor_interval从settings中加载
                    self.data['settings']['monitor_interval'] = settings.get('monitor_interval', 60)
                    self.config['monitor_interval'] = self.data['settings']['monitor_interval']
                    
                    # 打印加载状态
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"[{current_time}] 数据加载完成:")
                    print(f"- 已处理URLs数量: {len(self._processed_urls)}")
                    print(f"- 历史记录数量: {len(self._history)}")
                    print(f"- 检查间隔: {self.data['settings']['monitor_interval']}秒")
            else:
                self._init_default_data()
        except Exception as e:
            print(f"加载数据失败: {e}")
            self._init_default_data()

    def _init_default_data(self):
        """初始化默认数据"""
        self.data = {
            'processed_urls': [],
            'history': [],
            'settings': {
                'is_running': False,
                'ignore_old': False,
                'monitor_interval': 60,
                'retry': {
                    'enabled': False,
                    'max_attempts': 3,
                    'delay': 60
                },
                'rate_limit': {
                    'enabled': False,
                    'max_per_minute': 10
                },
                'schedule': {
                    'enabled': False,
                    'start_time': "09:00",
                    'end_time': "23:00"
                },
                'content_filter': {
                    'enabled': False,
                    'keywords': [],
                    'blacklist': [],
                    'whitelist': []
                },
                'backup': {
                    'enabled': False,
                    'interval': 24,
                    'max_backups': 5
                },
                'history_cleanup': {
                    'enabled': False,
                    'max_days': 30
                },
                'push_list': []  # 初始化推送列表
            },
            'statistics': {
                'daily': {},
                'total_pushes': 0,
                'failed_pushes': 0,
                'retry_pushes': 0
            },
            'sitemaps': [
                {
                    'name': '默认论坛',
                    'url': self.config.get('sitemap_url'),
                    'enabled': True
                }
            ],
            'templates': {
                'default': "📢 论坛新贴通知 📢\n━━━━━━━━━━━━━━\n📌 标题：{title}\n👤 作者：{author}\n🕒 时间：{time}\n🔗 链接：{url}\n━━━━━━━━━━━━━━\n💬 复制链接浏览器打开去评论吧",
                'simple': "新帖通知：{title} - {author}",
                'custom': []
            },
            'groups': {
                'default': {
                    'notify_groups': self.config.get('notify_groups', []),
                    'notify_users': self.config.get('notify_users', [])
                },
                'custom': {}
            }
        }
        self._processed_urls = set()
        self._history = []
        self._is_running = False
        self._ignore_old = False
        self._save_data()

    def _save_data(self):
        """保存数据到文件"""
        try:
            # 确保processed_urls是从_processed_urls集合转换而来
            self.data['processed_urls'] = list(self._processed_urls)
            # 确保历史记录被正确保存
            self.data['history'] = self._history
            # 保存所有设置
            self.data['settings'].update({
                'is_running': self._is_running,
                'ignore_old': self._ignore_old,
                'monitor_interval': self.config.get('monitor_interval', 60)
            })
            
            # 使用临时文件进行安全保存
            temp_file = self._data_file + '.tmp'
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=4)
            # 安全地替换原文件
            shutil.move(temp_file, self._data_file)
            
            # 打印保存状态（调试用）
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{current_time}] 数据已保存:")
            print(f"- 已处理URLs数量: {len(self._processed_urls)}")
            print(f"- 历史记录数量: {len(self._history)}")
            print(f"- 检查间隔: {self.data['settings']['monitor_interval']}秒")
            
        except Exception as e:
            print(f"保存数据失败: {e}")
            # Retry saving data if an error occurs
            try:
                with open(self._data_file, 'w', encoding='utf-8') as f:
                    json.dump(self.data, f, ensure_ascii=False, indent=4)
            except Exception as retry_e:
                print(f"重试保存数据失败: {retry_e}")

    def _start_monitor_thread(self):
        """启动监控线程"""
        if self._monitor_thread is None or not self._monitor_thread.is_alive():
            self._stop_event.clear()
            self._monitor_thread = Thread(target=self._monitor_loop, name="ForumMonitorThread", daemon=True)
            self._monitor_thread.start()
            
    def _stop_monitor_thread(self):
        """停止监控线程"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._stop_event.set()
            self._monitor_thread.join(timeout=1)
            
    def _monitor_loop(self):
        """监控循环"""
        last_check_time = time.time()
        try:
            while not self._stop_event.is_set():
                if self._is_running:
                    current_time = time.time()
                    # 确保距离上次检查至少间隔指定的时间
                    if current_time - last_check_time >= self.data['settings']['monitor_interval']:
                        self.check_sitemap()
                        last_check_time = current_time
                
                # 使用较长的睡眠时间，以便能够准确控制检查间隔
                time.sleep(self.data['settings']['monitor_interval'] / 2)
        except Exception as e:
            print(f"监控循环错误: {e}")
            
    def get_post_details(self, url):
        """从帖子URL获取详细信息"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 获取标题 - 直接获取h1.article-title下的a标签的title属性
            title = None
            title_link = soup.select_one('h1.article-title a')
            if title_link:
                title = title_link.get('title')  # 获取title属性值
            
            # 获取作者
            author = None
            author_elem = soup.select_one('.meta-left .display-name')
            if author_elem:
                author = author_elem.text.strip()
            
            return title or "获取失败", author or "获取失败"
        except Exception as e:
            print(f"获取帖子详情失败: {e}")
            return "获取失败", "获取失败"
            
    def convert_time(self, time_str):
        """转换时间为中国时间格式"""
        try:
            # 解析ISO格式时间
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            # 转换为中国时区
            china_tz = pytz.timezone('Asia/Shanghai')
            dt = dt.astimezone(china_tz)
            # 格式化输出
            return dt.strftime('%Y年%m月%d日 %H:%M:%S')
        except Exception as e:
            print(f"时间转换失败: {e}")
            return time_str
            
    def _check_rate_limit(self):
        """检查推送频率限制"""
        with self._rate_limit_lock:
            current_time = time.time()
            # 如果已经过了一分钟，重置计数器
            if current_time - self._push_reset_time >= 60:
                self._push_count = 0
                self._push_reset_time = current_time

            # 检查是否超过频率限制
            if self.data['settings']['rate_limit']['enabled']:
                max_per_minute = self.data['settings']['rate_limit']['max_per_minute']
                if self._push_count >= max_per_minute:
                    return False
                
            self._push_count += 1
            return True

    def process_post(self, url, lastmod=None, force=False):
        """处理帖子"""
        with self._processing_lock:
            # 检查帖子状态
            for record in self._history:
                if record['url'] == url:
                    if record['status'] in ['processing', 'completed'] and not force:
                        print(f"[跳过] 已处理的URL: {url}")
                        return False
                    # 更新状态为processing
                    record['status'] = 'processing'
                    break
            else:
                # 如果没有找到，添加新的记录
                china_time = self.convert_time(lastmod)
                self._history.append({
                    'time': china_time,
                    'title': '处理中...',
                    'author': '处理中...',
                    'url': url,
                    'status': 'processing'
                })
            self._save_data()

        success = False
        try:
            # 获取帖子详情
            title, author = self.get_post_details(url)
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{current_time}] 准备处理帖子: {title} ({url})")
            
            # 使用XML中的时间
            china_time = self.convert_time(lastmod) if lastmod else self.convert_time(datetime.now(pytz.UTC).isoformat())
            
            # 构建消息
            message = self._format_message(title, author, china_time, url)
            
            # 发送通知
            self.send_notifications(message)

            # 更新历史记录和处理状态
            with self._processing_lock:
                for record in self._history:
                    if record['url'] == url:
                        record.update({
                            'title': title,
                            'author': author,
                            'status': 'completed'
                        })
                        break
                self._processed_urls.add(url)
                self._save_data()
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{current_time}] ✅ 成功推送帖子: {title}")
            success = True
            return True
            
        except Exception as e:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{current_time}] 处理帖子失败: {e}")
            # 如果处理失败，清理所有相关状态
            with self._processing_lock:
                self._history[:] = [h for h in self._history if not (h['url'] == url and h['status'] == 'processing')]
                self._processed_urls.discard(url)
                self._save_data()
            return False
            
        finally:
            with self._processing_lock:
                self._processing_urls.discard(url)
                if not success:
                    self._history[:] = [h for h in self._history if not (h['url'] == url and h['status'] == 'processing')]
                    self._processed_urls.discard(url)
                    self._save_data()

    def check_sitemap(self, is_test=False):
        """检查sitemap获取新帖子"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{current_time}] {'[测试模式]' if is_test else '[正常模式]'} 开始检查sitemap")
        
        # 尝试获取检查锁，如果获取不到说明已经有检查在进行
        if not self._check_lock.acquire(blocking=False):
            print(f"[{current_time}] 已有检查正在进行，跳过本次检查")
            return

        try:
            # 重新加载数据，确保使用最新状态
            self._load_data()
            
            print(f"[{current_time}] 当前状态:")
            print(f"- 处理中URLs数量: {len(self._processing_urls)}")
            print(f"- 已处理URLs数量: {len(self._processed_urls)}")
            print(f"- 历史记录数量: {len(self._history)}")
            print(f"- 重试队列数量: {len(self._retry_queue)}")
            
            try:
                # 添加超时设置
                response = requests.get(self.config.get("sitemap_url"), timeout=10)
                response.raise_for_status()  # 检查响应状态
            except requests.RequestException as e:
                print(f"[{current_time}] 获取sitemap失败: {e}")
                return
            
            try:
                root = ET.fromstring(response.content)
            except ET.ParseError as e:
                print(f"[{current_time}] 解析sitemap失败: {e}")
                return
            
            # 获取所有URL条目并按时间排序
            urls = []
            with self._processing_lock:  # 使用锁检查URL状态
                for url in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
                    try:
                        loc = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
                        lastmod = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod').text
                        
                        # 检查是否已经在历史记录中（包括所有状态）
                        if not is_test and loc in self._processed_urls:
                            print(f"[{current_time}] 跳过已处理的URL: {loc}")
                            continue
                            
                        # 检查是否在处理中或重试队列中
                        if not is_test and (loc in self._processing_urls or loc in [item['url'] for item in self._retry_queue]):
                            print(f"[{current_time}] 跳过处理中的URL: {loc}")
                            continue
                            
                        urls.append((loc, lastmod))
                    except AttributeError:
                        continue
            
            # 如果没有新的URL，直接返回
            if not urls:
                print(f"[{current_time}] 没有新的帖子需要处理")
                return
                
            print(f"[{current_time}] 找到 {len(urls)} 个新帖子")
            
            # 按时间倒序排序
            urls.sort(key=lambda x: x[1], reverse=True)
            
            # 如果是测试模式，只处理最新的一条
            if is_test:
                loc, lastmod = urls[0]
                print(f"[{current_time}] 测试模式：处理最新的帖子")
                self.process_post(loc, lastmod, force=True)
                return
                
            # 正常模式，处理最新的帖子
            if urls and self._is_running:
                loc, lastmod = urls[0]
                print(f"[{current_time}] 开始处理最新的帖子")
                
                # 检查是否需要忽略旧帖子
                if self._ignore_old:
                    try:
                        post_time = datetime.fromisoformat(lastmod.replace('Z', '+00:00'))
                        ignore_time = self.data.get('ignore_time')
                        if ignore_time and post_time < datetime.fromisoformat(ignore_time):
                            print(f"[{current_time}] 跳过旧帖子")
                            return
                    except Exception as e:
                        print(f"[{current_time}] 时间比较错误: {e}")
                        return
                
                # 直接处理帖子，不使用新线程
                self.process_post(loc, lastmod)
            
        except Exception as e:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_msg = f"[{current_time}] 检查sitemap出错: {e}"
            print(error_msg)
            if is_test:
                self.wcf.send_text(error_msg, self.msg.sender, None)
        finally:
            self._check_lock.release()  # 释放检查锁

    def _is_recently_processed(self, url, time_window=60):
        """检查URL是否在最近一段时间内被处理过"""
        current_time = datetime.now()
        for history in reversed(self._history):
            if history['url'] == url:
                history_time = datetime.fromisoformat(history['time'])
                if (current_time - history_time).total_seconds() < time_window:
                    return True
        return False

    def _format_message(self, title, author, time, url):
        """格式化消息"""
        return (
            "📢 论坛新贴通知 📢\n"
            "━━━━━━━━━━━━━━\n"
            f"📌 标题：{title}\n"
            f"👤 作者：{author}\n"
            f"🕒 时间：{time}\n"
            f"🔗 链接：{url}\n"
            "━━━━━━━━━━━━━━\n"
            "💬 复制链接浏览器打开去评论吧！"
        )
    
    def export_history(self):
        """导出历史记录"""
        try:
            # 创建历史记录文本
            history_text = "📑 论坛监控历史记录\n" + "=" * 50 + "\n\n"
            for record in self._history:
                history_text += (
                    f"🕒 时间：{record['time']}\n"
                    f"📌 标题：{record['title']}\n"
                    f"👤 作者：{record['author']}\n"
                    f"🔗 链接：{record['url']}\n"
                    f"📝 状态：{'再次推送' if record.get('status') == 'reposted' else '首次推送'}\n"
                    + "-" * 30 + "\n"
                )
            
            # 直接发送文本消息而不是文件
            self.wcf.send_text(history_text, self.msg.sender, None)
            return True
        except Exception as e:
            print(f"导出历史记录失败: {e}")
            return False
            
    def send_notifications(self, message):
        """发送通知到配置的群和用户"""
        try:
            # 获取配置的群和用户
            notify_groups = self.config.get("notify_groups", [])
            notify_users = self.config.get("notify_users", [])
            
            # 确保列表中的所有ID都是字符串类型并去重
            unique_receivers = set(str(id) for id in notify_groups + notify_users)
            
            # 记录发送状态
            sent_count = 0
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            for receiver_id in unique_receivers:
                try:
                    self.wcf.send_text(message, receiver_id, None)
                    sent_count += 1
                except Exception as e:
                    print(f"[{current_time}] 发送到 {receiver_id} 失败: {e}")
            
            print(f"[{current_time}] 成功发送到 {sent_count}/{len(unique_receivers)} 个接收者")
            
        except Exception as e:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{current_time}] 发送通知失败: {e}")
            
    def show_help(self):
        """显示帮助菜单"""
        help_text = (
            "🤖 论坛监控机器人命令菜单 🤖\n"
            "━━━━━━━━━━━━━━\n"
            "📋 基础命令：\n"
            "• TS帮助 - 显示此菜单\n"
            "• TS测试 - 测试监控功能\n"
            "• TS状态 - 查看当前状态\n"
            "\n"
            "⚙️ 控制命令：\n"
            "• TS开启 - 开启推送\n"
            "• TS关闭 - 关闭推送\n"
            "• TS清理 - 清除URL缓存\n"
            "• TS间隔 <秒数> - 设置检查间隔\n"
            "\n"
            "📊 高级功能：\n"
            "• TS忽略旧帖 - 忽略历史帖子\n"
            "• TS历史记录 - 导出推送记录\n"
            "• TS推送 <URL> - 再次推送指定URL的帖子\n"
            "\n"
            "💾 备份功能：\n"
            "• TS备份 - 手动备份数据\n"
            "• TS备份设置 开启/关闭 - 自动备份开关\n"
            "• TS备份间隔 <小时> - 设置备份间隔\n"
            "• TS备份数量 <数量> - 设置保留数量\n"
            "\n"
            "🔄 重试机制：\n"
            "• TS重试 开启/关闭 - 失败重试开关\n"
            "• TS重试次数 <次数> - 设置重试次数\n"
            "• TS重试间隔 <秒数> - 设置重试间隔\n"
            "\n"
            "⏱️ 推送控制：\n"
            "• TS频率 开启/关闭 - 频率限制开关\n"
            "• TS频率设置 <次数/分钟> - 设置推送频率\n"
            "• TS时段 开启/关闭 - 时段限制开关\n"
            "• TS时段设置 <开始> <结束> - 设置推送时段\n"
            "\n"
            "🔍 内容过滤：\n"
            "• TS过滤 开启/关闭 - 内容过滤开关\n"
            "• TS过滤词 添加/删除 <关键词> - 管理关键词\n"
            "• TS过滤词列表 - 查看过滤关键词\n"
            "\n"
            "📡 数据源管理：\n"
            "• TS源 添加 <名称> <URL> - 添加数据源\n"
            "• TS源 删除 <名称> - 删除数据源\n"
            "• TS源 列表 - 查看所有数据源\n"
            "• TS源 开启/关闭 <名称> - 控制数据源\n"
            "\n"
            "📝 推送模板：\n"
            "• TS模板 添加 <名称> <内容> - 添加模板\n"
            "• TS模板 删除 <名称> - 删除模板\n"
            "• TS模板 列表 - 查看所有模板\n"
            "• TS模板 设置 <名称> - 设置当前模板\n"
            "\n"
            "👥 分组管理：\n"
            "• TS分组 创建 <名称> - 创建推送分组\n"
            "• TS分组 删除 <名称> - 删除推送分组\n"
            "• TS分组 添加 <分组> <ID> - 添加推送对象\n"
            "• TS分组 移除 <分组> <ID> - 移除推送对象\n"
            "• TS分组 列表 - 查看所有分组\n"
            "\n"
            "🗑️ 历史管理：\n"
            "• TS历史清理 开启/关闭 - 自动清理开关\n"
            "• TS历史天数 <天数> - 设置保留天数\n"
            "• TS历史立即清理 - 立即清理记录\n"
            "━━━━━━━━━━━━━━"
        )
        self.send_response(help_text)

    def send_response(self, message):
        """根据消息来源发送回复"""
        if self.msg.roomid:
            # 如果是群聊消息，回复到群聊
            target_id = self.msg.roomid
        else:
            # 如果是个人消息，回复给个人
            target_id = self.msg.sender
        self.wcf.send_text(message, target_id, None)

    def deal_msg(self):
        """处理消息"""
        try:
            if not self.msg.content.startswith("TS"):
                return
                
            full_cmd = self.msg.content.strip()
            
            # 基础命令处理
            if full_cmd == "TS帮助":
                self.show_help()
            elif full_cmd == "TS推送列表":
                # 查看推送列表
                push_list = self.data.get('push_list', [])
                if not push_list:
                    self.send_response("推送列表为空")
                else:
                    group_ids = [id for id in push_list if "@chatroom" in id]
                    user_ids = [id for id in push_list if "@chatroom" not in id]
                    response = "📋 推送列表：\n"
                    response += "群聊ID：\n" + "\n".join(group_ids) + "\n"
                    response += "个人ID：\n" + "\n".join(user_ids)
                    self.send_response(response)
            elif full_cmd == "TSID":
                # 获取群ID
                if self.msg.roomid:
                    group_id = self.msg.roomid
                else:
                    group_id = self.msg.sender
                # 获取发送者的个人ID
                sender_id = self.msg.sender
                # 打印群ID和个人ID
                print(f"群ID: {group_id}, 个人ID: {sender_id}")
                # 获取管理员列表
                admin_list = self.config.get("manager_wxid", [])
                # 给每个管理员发送私信
                for admin_id in admin_list:
                    self.wcf.send_text(f"群ID: {group_id}", admin_id, None)
                # 在群里回复一个简单的确认
                self.send_response("✅ 已发送群ID到管理员")
            elif full_cmd == "TS测试":
                self.check_sitemap(is_test=True)
            elif full_cmd == "TS清理":
                old_count = len(self._processed_urls)
                self._processed_urls.clear()
                self._history.clear()  # 同时清理历史记录
                self._save_data()
                self.send_response(f"已清除URL缓存和历史记录，共清除{old_count}条记录")
            elif full_cmd == "TS开启":
                self._is_running = True
                self._start_monitor_thread()
                self._save_data()
                self.send_response("✅ 已开启论坛监控推送")
            elif full_cmd == "TS关闭":
                self._is_running = False
                self._stop_monitor_thread()
                self._save_data()
                self.send_response("⛔ 已关闭论坛监控推送")
            elif full_cmd == "TS忽略旧帖":
                self._ignore_old = True
                self.data['ignore_time'] = datetime.now(pytz.UTC).isoformat()
                self._save_data()
                self.send_response("✅ 已设置忽略当前时间之前的帖子")
            elif full_cmd.startswith("TS推送 "):
                url = full_cmd[5:].strip()
                if url:
                    if self.process_post(url, force=True):
                        self.send_response("✅ 推送完成")
                    else:
                        self.send_response("❌ 推送失败")
                else:
                    self.send_response("❌ 请提供要推送的帖子URL")
            elif full_cmd.startswith("TS间隔 "):
                try:
                    interval = int(full_cmd.split(" ")[1])
                    if interval < 10:
                        self.send_response("❌ 间隔时间不能小于10秒")
                        return
                    # 更新所有相关的间隔时间设置
                    self.config["monitor_interval"] = interval
                    self.data['settings']['monitor_interval'] = interval
                    self._save_data()  # 确保设置被保存
                    # 重启监控线程以应用新的间隔时间
                    if self._is_running:
                        self._stop_monitor_thread()
                        self._start_monitor_thread()
                    self.send_response(f"✅ 已设置监控间隔为{interval}秒")
                except ValueError:
                    self.send_response("❌ 请输入有效的数字")
            elif full_cmd == "TS状态":
                status = (
                    "📊 论坛监控状态\n"
                    "━━━━━━━━━━━━━━\n"
                    f"推送开关：{'✅ 开启' if self._is_running else '⛔ 关闭'}\n"
                    f"忽略旧帖：{'✅ 是' if self._ignore_old else '❌ 否'}\n"
                    f"检查间隔：{self.data['settings']['monitor_interval']}秒\n"
                    f"已处理URL：{len(self._processed_urls)} 条\n"
                    f"历史记录数：{len(self._history)} 条\n"
                    "━━━━━━━━━━━━━━"
                )
                self.send_response(status)
            
            # 备份功能命令
            elif full_cmd == "TS备份":
                self._create_backup()
                self.send_response("✅ 已完成手动备份")
            elif full_cmd == "TS备份设置 开启":
                self.data['settings']['backup']['enabled'] = True
                self._save_data()
                self._start_backup_thread()
                self.send_response("✅ 已开启自动备份")
            elif full_cmd == "TS备份设置 关闭":
                self.data['settings']['backup']['enabled'] = False
                self._save_data()
                self.send_response("⛔ 已关闭自动备份")
            elif full_cmd.startswith("TS备份间隔 "):
                try:
                    hours = int(full_cmd.split(" ")[1])
                    self.data['settings']['backup']['interval'] = hours * 3600
                    self._save_data()
                    self.send_response(f"✅ 已设置备份间隔为{hours}小时")
                except:
                    self.send_response("❌ 请指定有效的小时数")
            elif full_cmd.startswith("TS备份数量 "):
                try:
                    count = int(full_cmd.split(" ")[1])
                    self.data['settings']['backup']['max_backups'] = count
                    self._save_data()
                    self.send_response(f"✅ 已设置保留{count}个备份")
                except:
                    self.send_response("❌ 请指定有效的备份数量")
            
            # 重试机制命令
            elif full_cmd == "TS重试 开启":
                self.data['settings']['retry']['enabled'] = True
                self._save_data()
                self._start_retry_thread()
                self.send_response("✅ 已开启失败重试")
            elif full_cmd == "TS重试 关闭":
                self.data['settings']['retry']['enabled'] = False
                self._save_data()
                self.send_response("⛔ 已关闭失败重试")
            elif full_cmd.startswith("TS重试次数 "):
                try:
                    attempts = int(full_cmd.split(" ")[1])
                    self.data['settings']['retry']['max_attempts'] = attempts
                    self._save_data()
                    self.send_response(f"✅ 已设置最大重试次数为{attempts}次")
                except:
                    self.send_response("❌ 请指定有效的重试次数")
            elif full_cmd.startswith("TS重试间隔 "):
                try:
                    delay = int(full_cmd.split(" ")[1])
                    self.data['settings']['retry']['delay'] = delay
                    self._save_data()
                    self.send_response(f"✅ 已设置重试间隔为{delay}秒")
                except:
                    self.send_response("❌ 请指定有效的间隔秒数")
            
            # 推送控制命令
            elif full_cmd == "TS频率 开启":
                self.data['settings']['rate_limit']['enabled'] = True
                self._save_data()
                self.send_response("✅ 已开启推送频率限制")
            elif full_cmd == "TS频率 关闭":
                self.data['settings']['rate_limit']['enabled'] = False
                self._save_data()
                self.send_response("⛔ 已关闭推送频率限制")
            elif full_cmd.startswith("TS频率设置 "):
                try:
                    rate = int(full_cmd.split(" ")[1])
                    self.data['settings']['rate_limit']['max_per_minute'] = rate
                    self._save_data()
                    self.send_response(f"✅ 已设置最大推送频率为每分钟{rate}次")
                except:
                    self.send_response("❌ 请指定有效的推送频率")
            
            # 时间段设置命令
            elif full_cmd == "TS时段 开启":
                self.data['settings']['schedule']['enabled'] = True
                self._save_data()
                self.send_response("✅ 已开启时间段限制")
            elif full_cmd == "TS时段 关闭":
                self.data['settings']['schedule']['enabled'] = False
                self._save_data()
                self.send_response("⛔ 已关闭时间段限制")
            elif full_cmd.startswith("TS时段设置 "):
                try:
                    times = full_cmd.split(" ")[1:]
                    if len(times) == 2:
                        self.data['settings']['schedule']['start_time'] = times[0]
                        self.data['settings']['schedule']['end_time'] = times[1]
                        self._save_data()
                        self.send_response(f"✅ 已设置推送时段为{times[0]}至{times[1]}")
                    else:
                        self.send_response("❌ 请指定开始和结束时间，格式：TS时段设置 09:00 23:00")
                except:
                    self.send_response("❌ 请使用正确的时间格式（HH:MM）")
            
            # 内容过滤命令
            elif full_cmd == "TS过滤 开启":
                self.data['settings']['content_filter']['enabled'] = True
                self._save_data()
                self.send_response("✅ 已开启内容过滤")
            elif full_cmd == "TS过滤 关闭":
                self.data['settings']['content_filter']['enabled'] = False
                self._save_data()
                self.send_response("⛔ 已关闭内容过滤")
            elif full_cmd.startswith("TS过滤词 添加 "):
                keyword = full_cmd[8:].strip()
                if keyword:
                    if keyword not in self.data['settings']['content_filter']['keywords']:
                        self.data['settings']['content_filter']['keywords'].append(keyword)
                        self._save_data()
                        self.send_response(f"✅ 已添加过滤关键词：{keyword}")
                    else:
                        self.send_response("❌ 该关键词已存在")
                else:
                    self.send_response("❌ 请指定要添加的关键词")
            elif full_cmd.startswith("TS过滤词 删除 "):
                keyword = full_cmd[8:].strip()
                if keyword in self.data['settings']['content_filter']['keywords']:
                    self.data['settings']['content_filter']['keywords'].remove(keyword)
                    self._save_data()
                    self.send_response(f"✅ 已删除过滤关键词：{keyword}")
                else:
                    self.send_response("❌ 未找到该关键词")
            elif full_cmd == "TS过滤词列表":
                keywords = self.data['settings']['content_filter']['keywords']
                if keywords:
                    keyword_list = "\n".join([f"• {kw}" for kw in keywords])
                    self.send_response(f"📝 当前过滤关键词：\n{keyword_list}")
                else:
                    self.send_response("📝 当前没有设置过滤关键词")
            
            # 数据源管理命令
            elif full_cmd.startswith("TS源 添加 "):
                try:
                    _, name, url = full_cmd.split(" ", 2)
                    if not any(s['name'] == name for s in self.data['sitemaps']):
                        self.data['sitemaps'].append({
                            'name': name,
                            'url': url,
                            'enabled': True
                        })
                        self._save_data()
                        self.send_response(f"✅ 已添加数据源：{name}")
                    else:
                        self.send_response("❌ 该数据源名称已存在")
                except ValueError:
                    self.send_response("❌ 格式错误，请使用：TS源 添加 <名称> <URL>")
            elif full_cmd.startswith("TS源 删除 "):
                name = full_cmd[6:].strip()
                for i, sitemap in enumerate(self.data['sitemaps']):
                    if sitemap['name'] == name:
                        del self.data['sitemaps'][i]
                        self._save_data()
                        self.send_response(f"✅ 已删除数据源：{name}")
                        break
                else:
                    self.send_response("❌ 未找到该数据源")
            elif full_cmd == "TS源 列表":
                if self.data['sitemaps']:
                    sitemap_list = "\n".join([
                        f"• {s['name']}: {s['url']} ({'启用' if s['enabled'] else '禁用'})"
                        for s in self.data['sitemaps']
                    ])
                    self.send_response(f"📡 数据源列表：\n{sitemap_list}")
                else:
                    self.send_response("📡 当前没有配置数据源")
            elif full_cmd.startswith("TS源 开启 ") or full_cmd.startswith("TS源 关闭 "):
                name = full_cmd[6:].strip()
                enable = full_cmd.startswith("TS源 开启 ")
                for sitemap in self.data['sitemaps']:
                    if sitemap['name'] == name:
                        sitemap['enabled'] = enable
                        self._save_data()
                        self.send_response(f"{'✅ 已启用' if enable else '⛔ 已禁用'}数据源：{name}")
                        break
                else:
                    self.send_response("❌ 未找到该数据源")
            
            # 推送模板命令
            elif full_cmd.startswith("TS模板 添加 "):
                try:
                    _, name, content = full_cmd[8:].split(" ", 1)
                    if name not in self.data['templates']:
                        self.data['templates'][name] = content
                        self._save_data()
                        self.send_response(f"✅ 已添加模板：{name}")
                    else:
                        self.send_response("❌ 该模板名称已存在")
                except ValueError:
                    self.send_response("❌ 格式错误，请使用：TS模板 添加 <名称> <内容>")
            elif full_cmd.startswith("TS模板 删除 "):
                name = full_cmd[8:].strip()
                if name in self.data['templates'] and name not in ['default', 'simple']:
                    del self.data['templates'][name]
                    self._save_data()
                    self.send_response(f"✅ 已删除模板：{name}")
                else:
                    self.send_response("❌ 无法删除该模板")
            elif full_cmd == "TS模板 列表":
                template_list = "\n".join([f"• {name}" for name in self.data['templates'].keys()])
                self.send_response(f"📝 可用模板列表：\n{template_list}")
            elif full_cmd.startswith("TS模板 设置 "):
                name = full_cmd[8:].strip()
                if name in self.data['templates']:
                    self.data['current_template'] = name
                    self._save_data()
                    self.send_response(f"✅ 已设置当前模板为：{name}")
                else:
                    self.send_response("❌ 未找到该模板")
            
            # 分组管理命令
            elif full_cmd.startswith("TS分组 创建 "):
                name = full_cmd[8:].strip()
                if name not in self.data['groups']['custom']:
                    self.data['groups']['custom'][name] = {
                        'notify_groups': [],
                        'notify_users': []
                    }
                    self._save_data()
                    self.send_response(f"✅ 已创建分组：{name}")
                else:
                    self.send_response("❌ 该分组名称已存在")
            elif full_cmd.startswith("TS分组 删除 "):
                name = full_cmd[8:].strip()
                if name in self.data['groups']['custom']:
                    del self.data['groups']['custom'][name]
                    self._save_data()
                    self.send_response(f"✅ 已删除分组：{name}")
                else:
                    self.send_response("❌ 未找到该分组")
            elif full_cmd.startswith("TS分组 添加 "):
                try:
                    _, group_name, target_id = full_cmd[8:].split(" ", 1)
                    if group_name in self.data['groups']['custom']:
                        if '@chatroom' in target_id:
                            if target_id not in self.data['groups']['custom'][group_name]['notify_groups']:
                                self.data['groups']['custom'][group_name]['notify_groups'].append(target_id)
                        else:
                            if target_id not in self.data['groups']['custom'][group_name]['notify_users']:
                                self.data['groups']['custom'][group_name]['notify_users'].append(target_id)
                        self._save_data()
                        self.send_response(f"✅ 已添加推送对象到分组：{group_name}")
                    else:
                        self.send_response("❌ 未找到该分组")
                except ValueError:
                    self.send_response("❌ 格式错误，请使用：TS分组 添加 <分组> <群ID/用户ID>")
            elif full_cmd.startswith("TS分组 移除 "):
                try:
                    _, group_name, target_id = full_cmd[8:].split(" ", 1)
                    if group_name in self.data['groups']['custom']:
                        if '@chatroom' in target_id:
                            if target_id in self.data['groups']['custom'][group_name]['notify_groups']:
                                self.data['groups']['custom'][group_name]['notify_groups'].remove(target_id)
                        else:
                            if target_id in self.data['groups']['custom'][group_name]['notify_users']:
                                self.data['groups']['custom'][group_name]['notify_users'].remove(target_id)
                        self._save_data()
                        self.send_response(f"✅ 已从分组移除推送对象：{group_name}")
                    else:
                        self.send_response("❌ 未找到该分组")
                except ValueError:
                    self.send_response("❌ 格式错误，请使用：TS分组 移除 <分组> <群ID/用户ID>")
            elif full_cmd == "TS分组 列表":
                group_list = ["📋 推送分组列表："]
                # 添加默认分组信息
                group_list.append("\n默认分组：")
                group_list.append(f"• 群聊：{len(self.data['groups']['default']['notify_groups'])}个")
                group_list.append(f"• 用户：{len(self.data['groups']['default']['notify_users'])}个")
                # 添加自定义分组信息
                if self.data['groups']['custom']:
                    group_list.append("\n自定义分组：")
                    for name, group in self.data['groups']['custom'].items():
                        group_list.append(f"• {name}:")
                        group_list.append(f"  - 群聊：{len(group['notify_groups'])}个")
                        group_list.append(f"  - 用户：{len(group['notify_users'])}个")
                self.send_response("\n".join(group_list))
            
            # 历史记录管理命令
            elif full_cmd == "TS历史清理 开启":
                self.data['settings']['history_cleanup']['enabled'] = True
                self._save_data()
                self.send_response("✅ 已开启历史记录自动清理")
            elif full_cmd == "TS历史清理 关闭":
                self.data['settings']['history_cleanup']['enabled'] = False
                self._save_data()
                self.send_response("⛔ 已关闭历史记录自动清理")
            elif full_cmd.startswith("TS历史天数 "):
                try:
                    days = int(full_cmd.split(" ")[1])
                    if days > 0:
                        self.data['settings']['history_cleanup']['max_days'] = days
                        self._save_data()
                        self.send_response(f"✅ 已设置历史记录保留{days}天")
                    else:
                        self.send_response("❌ 保留天数必须大于0")
                except:
                    self.send_response("❌ 请指定有效的天数")
            elif full_cmd == "TS历史立即清理":
                self._cleanup_history()
                self.send_response("✅ 已执行历史记录清理")
            elif full_cmd == "TS历史记录":
                self.send_response("正在导出历史记录...")
                if self.export_history():
                    history_text = self.format_history()
                    self.send_response(history_text)
                else:
                    self.send_response("❌ 导出历史记录失败")
            
            elif full_cmd.startswith("TS添加推送"):
                # 获取群ID
                if self.msg.roomid:
                    group_id = self.msg.roomid
                else:
                    self.send_response("❌ 请在群聊中使用此命令")
                    return
                # 添加群ID到推送列表
                if group_id not in self.data['push_list']:
                    self.data['push_list'].append(group_id)
                    self._save_data()
                    self.send_response("✅ 已添加群ID到推送列表")
                else:
                    self.send_response("❌ 群ID已在推送列表中")

            elif full_cmd.startswith("TS删除推送"):
                # 获取群ID
                if self.msg.roomid:
                    group_id = self.msg.roomid
                else:
                    self.send_response("❌ 请在群聊中使用此命令")
                    return
                # 从推送列表中删除群ID
                if group_id in self.data['push_list']:
                    self.data['push_list'].remove(group_id)
                    self._save_data()
                    self.send_response("✅ 已从推送列表中删除群ID")
                else:
                    self.send_response("❌ 群ID不在推送列表中")
            
            else:
                self.send_response("❌ 未知命令，请使用 TS帮助 查看可用命令")
        except Exception as e:
            print(f"处理消息错误: {e}")
            self.send_response(f"❌ 命令执行出错: {str(e)}")

    def filter_msg(self) -> bool:
        """消息过滤"""
        # 如果是命令，需要处理
        if self.msg.type == 1 and self.msg.content.startswith("TS"):
            # 检查发送者是否在管理员列表中
            if self.msg.sender in self.config.get("manager_wxid", []):
                return True
            else:
                self.wcf.send_text("❌ 抱歉，您没有执行命令的权限", self.msg.sender, None)
                return False
                
        # 其他情况返回False，不执行任何操作
        return False

    def run(self):
        """插件运行入口"""
        try:
            self.init_config_data()
            if self.filter_msg():
                self.deal_msg()
        except Exception as e:
            print(f"插件运行错误: {e}")

    def __del__(self):
        """析构函数，确保线程正确退出"""
        try:
            self._stop_event.set()  # 停止所有线程
            if self._monitor_thread:
                self._monitor_thread.join(timeout=1)
            if self._backup_thread:
                self._backup_thread.join(timeout=1)
            if self._retry_thread:
                self._retry_thread.join(timeout=1)
            if self._cleanup_thread:
                self._cleanup_thread.join(timeout=1)
            self._save_data()
        except Exception as e:
            print(f"插件清理错误: {e}")

    def _start_backup_thread(self):
        """启动备份线程"""
        if self._backup_thread is None or not self._backup_thread.is_alive():
            self._backup_thread = Thread(target=self._backup_loop, name="BackupThread", daemon=True)
            self._backup_thread.start()

    def _start_retry_thread(self):
        """启动重试线程"""
        if self._retry_thread is None or not self._retry_thread.is_alive():
            self._retry_thread = Thread(target=self._retry_loop, name="RetryThread", daemon=True)
            self._retry_thread.start()

    def _start_cleanup_thread(self):
        """启动清理线程"""
        if self._cleanup_thread is None or not self._cleanup_thread.is_alive():
            self._cleanup_thread = Thread(target=self._cleanup_loop, name="CleanupThread", daemon=True)
            self._cleanup_thread.start()

    def _backup_loop(self):
        """备份循环"""
        while not self._stop_event.is_set():
            if self.data['settings']['backup']['enabled']:
                self._create_backup()
            self._stop_event.wait(self.data['settings']['backup']['interval'])

    def _retry_loop(self):
        """重试循环"""
        while not self._stop_event.is_set():
            if self.data['settings']['retry']['enabled'] and self._retry_queue:
                self._process_retry_queue()
            self._stop_event.wait(self.data['settings']['retry']['delay'])

    def _cleanup_loop(self):
        """清理循环"""
        while not self._stop_event.is_set():
            if self.data['settings']['history_cleanup']['enabled']:
                self._cleanup_history()
            self._stop_event.wait(86400)  # 每天检查一次

    def _create_backup(self):
        """创建备份"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(self._backup_dir, f'backup_{timestamp}.json')
            shutil.copy2(self._data_file, backup_file)
            
            # 清理旧备份
            backups = sorted([f for f in os.listdir(self._backup_dir) if f.startswith('backup_')])
            max_backups = self.data['settings']['backup']['max_backups']
            if len(backups) > max_backups:
                for old_backup in backups[:-max_backups]:
                    os.remove(os.path.join(self._backup_dir, old_backup))
        except Exception as e:
            print(f"创建备份失败: {e}")

    def _process_retry_queue(self):
        """处理重试队列"""
        if not self._retry_queue:
            return
            
        retry_item = self._retry_queue[0]
        if retry_item['attempts'] < self.data['settings']['retry']['max_attempts']:
            success = self.process_post(retry_item['url'], retry_item.get('lastmod'))
            if success:
                self._retry_queue.pop(0)
            else:
                retry_item['attempts'] += 1
        else:
            self._retry_queue.pop(0)
            self.data['statistics']['failed_pushes'] += 1
            self._save_data()

    def _cleanup_history(self):
        """清理历史记录"""
        if not self.data['settings']['history_cleanup']['enabled']:
            return
            
        max_days = self.data['settings']['history_cleanup']['max_days']
        cutoff_date = datetime.now() - timedelta(days=max_days)
        
        new_history = []
        for record in self._history:
            try:
                record_date = datetime.strptime(record['time'], '%Y年%m月%d日 %H:%M:%S')
                if record_date > cutoff_date:
                    new_history.append(record)
            except:
                new_history.append(record)
        
        self._history = new_history
        self._save_data()

    def format_history(self):
        """格式化历史记录"""
        history_text = "📑 论坛监控历史记录\n==================================================\n"
        for record in self._history:
            history_text += (
                f"\n🕒 时间：{record['time']}\n"
                f"📌 标题：{record['title']}\n"
                f"👤 作者：{record['author']}\n"
                f"🔗 链接：{record['url']}\n"
                f"📝 状态：{record['status']}\n"
                "------------------------------"
            )
        return history_text