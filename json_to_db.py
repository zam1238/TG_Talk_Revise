#!/usr/bin/env python3
"""
JSON 数据迁移工具
将旧的 JSON 文件数据迁移到 SQLite 数据库

使用方法:
    python3 migrate_json_to_db.py [JSON文件夹路径]
    
示例:
    python3 migrate_json_to_db.py /path/to/old_bot_folder
    python3 migrate_json_to_db.py  # 默认使用当前目录
"""

import os
import json
import sys
import sqlite3
from datetime import datetime

# 导入数据库模块
try:
    import database as db
except ImportError:
    print("❌ 错误: 找不到 database.py 模块")
    print("💡 请确保在 host_bot.py 所在目录运行此脚本")
    sys.exit(1)


class JSONMigrator:
    """JSON 数据迁移工具"""
    
    def __init__(self, json_folder=None):
        """
        初始化迁移工具
        
        Args:
            json_folder: JSON 文件夹路径，默认为当前目录
        """
        self.json_folder = json_folder or os.getcwd()
        self.stats = {
            'bots': 0,
            'mappings': 0,
            'verified_users': 0,
            'blacklist': 0,
            'errors': []
        }
        
        print(f"📂 JSON 文件夹: {self.json_folder}")
        print(f"💾 数据库文件: {db.DB_FILE}")
        print()
    
    def load_json_file(self, filename):
        """加载 JSON 文件"""
        filepath = os.path.join(self.json_folder, filename)
        if not os.path.exists(filepath):
            print(f"⚠️  {filename} 不存在，跳过")
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"✅ 已加载 {filename}")
            return data
        except Exception as e:
            error_msg = f"❌ 加载 {filename} 失败: {e}"
            print(error_msg)
            self.stats['errors'].append(error_msg)
            return None
    
    def migrate_bots(self):
        """迁移 bots.json"""
        print("\n" + "="*50)
        print("📦 开始迁移 Bot 配置 (bots.json)")
        print("="*50)
        
        bots_data = self.load_json_file('bots.json')
        if not bots_data:
            return
        
        # 遍历所有 owner_id
        for owner_id, owner_data in bots_data.items():
            bots_list = owner_data.get('bots', [])
            
            for bot in bots_list:
                bot_username = bot.get('bot_username')
                token = bot.get('token')
                welcome_msg = bot.get('welcome_msg', '')
                mode = bot.get('mode', 'direct')
                forum_group_id = bot.get('forum_group_id')
                
                if not bot_username or not token:
                    error_msg = f"⚠️  跳过无效 Bot 数据: {bot}"
                    print(error_msg)
                    self.stats['errors'].append(error_msg)
                    continue
                
                try:
                    # 检查 Bot 是否已存在
                    existing_bot = db.get_bot(bot_username)
                    if existing_bot:
                        print(f"  ⏭️  Bot @{bot_username} 已存在，跳过")
                        continue
                    
                    # 1. 添加基本信息到数据库
                    db.add_bot(
                        bot_username=bot_username,
                        token=token,
                        owner=int(owner_id),
                        welcome_msg=welcome_msg
                    )
                    
                    # 2. 更新模式（如果不是默认值）
                    if mode and mode != 'direct':
                        db.update_bot_mode(bot_username, mode)
                    
                    # 3. 更新话题群ID（如果有）
                    if forum_group_id:
                        db.update_bot_forum_id(bot_username, forum_group_id)
                    
                    print(f"  ✅ Bot @{bot_username} (Owner: {owner_id}, Mode: {mode})")
                    self.stats['bots'] += 1
                    
                except Exception as e:
                    error_msg = f"  ❌ 添加 Bot @{bot_username} 失败: {e}"
                    print(error_msg)
                    self.stats['errors'].append(error_msg)
        
        print(f"\n✅ Bot 迁移完成: {self.stats['bots']} 个")
    
    def migrate_mappings(self):
        """迁移 msg_map.json"""
        print("\n" + "="*50)
        print("🗺️  开始迁移消息映射 (msg_map.json)")
        print("="*50)
        
        msg_map = self.load_json_file('msg_map.json')
        if not msg_map:
            return
        
        for bot_username, mappings in msg_map.items():
            print(f"\n  📱 Bot: @{bot_username}")
            
            # 检查 Bot 是否存在
            if not db.get_bot(bot_username):
                error_msg = f"    ⚠️  Bot @{bot_username} 不存在，跳过映射"
                print(error_msg)
                self.stats['errors'].append(error_msg)
                continue
            
            count = 0
            
            # 1. 迁移 direct 映射
            direct_map = mappings.get('direct', {})
            for user_msg_id, owner_msg_id in direct_map.items():
                try:
                    db.set_mapping(bot_username, "direct", user_msg_id, owner_msg_id)
                    count += 1
                except Exception as e:
                    error_msg = f"    ❌ 设置 direct 映射失败: {e}"
                    self.stats['errors'].append(error_msg)
            
            # 2. 迁移 topics 映射
            topics_map = mappings.get('topics', {})
            for user_id, topic_id in topics_map.items():
                try:
                    db.set_mapping(bot_username, "topic", user_id, str(topic_id))
                    count += 1
                except Exception as e:
                    error_msg = f"    ❌ 设置 topic 映射失败: {e}"
                    self.stats['errors'].append(error_msg)
            
            # 3. 迁移 user_to_forward 映射
            user_forward = mappings.get('user_to_forward', {})
            for user_msg_id, forward_msg_id in user_forward.items():
                try:
                    db.set_mapping(bot_username, "user_forward", user_msg_id, forward_msg_id)
                    count += 1
                except Exception as e:
                    error_msg = f"    ❌ 设置 user_forward 映射失败: {e}"
                    self.stats['errors'].append(error_msg)
            
            # 4. 迁移 forward_to_user 映射
            forward_user = mappings.get('forward_to_user', {})
            for forward_msg_id, user_msg_id in forward_user.items():
                try:
                    db.set_mapping(bot_username, "forward_user", forward_msg_id, user_msg_id)
                    count += 1
                except Exception as e:
                    error_msg = f"    ❌ 设置 forward_user 映射失败: {e}"
                    self.stats['errors'].append(error_msg)
            
            # 5. 迁移 owner_to_user 映射
            owner_user = mappings.get('owner_to_user', {})
            for owner_msg_id, user_msg_id in owner_user.items():
                try:
                    db.set_mapping(bot_username, "owner_user", owner_msg_id, user_msg_id)
                    count += 1
                except Exception as e:
                    error_msg = f"    ❌ 设置 owner_user 映射失败: {e}"
                    self.stats['errors'].append(error_msg)
            
            print(f"    ✅ {count} 条映射")
            self.stats['mappings'] += count
        
        print(f"\n✅ 消息映射迁移完成: {self.stats['mappings']} 条")
    
    def migrate_verified_users(self):
        """迁移 verified_users.json"""
        print("\n" + "="*50)
        print("✅ 开始迁移已验证用户 (verified_users.json)")
        print("="*50)
        
        verified_data = self.load_json_file('verified_users.json')
        if not verified_data:
            return
        
        for bot_username, users in verified_data.items():
            print(f"\n  📱 Bot: @{bot_username}")
            
            # 检查 Bot 是否存在
            if not db.get_bot(bot_username):
                error_msg = f"    ⚠️  Bot @{bot_username} 不存在，跳过验证用户"
                print(error_msg)
                self.stats['errors'].append(error_msg)
                continue
            
            count = 0
            
            # 处理列表格式
            if isinstance(users, list):
                for user_id in users:
                    try:
                        # 检查是否已存在
                        if db.is_verified(bot_username, user_id):
                            continue
                        
                        db.add_verified_user(bot_username, user_id, '', '')
                        count += 1
                    except Exception as e:
                        error_msg = f"    ❌ 添加验证用户 {user_id} 失败: {e}"
                        self.stats['errors'].append(error_msg)
            
            # 处理字典格式
            elif isinstance(users, dict):
                for user_id_str, user_info in users.items():
                    try:
                        user_id = int(user_id_str)
                        user_name = user_info.get('user_name', '')
                        user_username = user_info.get('user_username', '')
                        
                        # 检查是否已存在
                        if db.is_verified(bot_username, user_id):
                            continue
                        
                        db.add_verified_user(bot_username, user_id, user_name, user_username)
                        count += 1
                    except Exception as e:
                        error_msg = f"    ❌ 添加验证用户 {user_id_str} 失败: {e}"
                        self.stats['errors'].append(error_msg)
            
            print(f"    ✅ {count} 个验证用户")
            self.stats['verified_users'] += count
        
        print(f"\n✅ 验证用户迁移完成: {self.stats['verified_users']} 个")
    
    def migrate_blacklist(self):
        """迁移 blacklist.json"""
        print("\n" + "="*50)
        print("🚫 开始迁移黑名单 (blacklist.json)")
        print("="*50)
        
        blacklist_data = self.load_json_file('blacklist.json')
        if not blacklist_data:
            return
        
        for bot_username, user_ids in blacklist_data.items():
            print(f"\n  📱 Bot: @{bot_username}")
            
            # 检查 Bot 是否存在
            if not db.get_bot(bot_username):
                error_msg = f"    ⚠️  Bot @{bot_username} 不存在，跳过黑名单"
                print(error_msg)
                self.stats['errors'].append(error_msg)
                continue
            
            count = 0
            for user_id in user_ids:
                try:
                    # 检查是否已在黑名单
                    if db.is_blacklisted(bot_username, user_id):
                        continue
                    
                    db.add_to_blacklist(bot_username, user_id)
                    count += 1
                except Exception as e:
                    error_msg = f"    ❌ 添加黑名单 {user_id} 失败: {e}"
                    self.stats['errors'].append(error_msg)
            
            print(f"    ✅ {count} 个黑名单用户")
            self.stats['blacklist'] += count
        
        print(f"\n✅ 黑名单迁移完成: {self.stats['blacklist']} 个")
    
    def run(self):
        """执行完整迁移"""
        print("\n" + "="*60)
        print("🚀 开始 JSON → SQLite 数据迁移")
        print("="*60)
        print()
        
        # 检查 JSON 文件夹是否存在
        if not os.path.exists(self.json_folder):
            print(f"❌ 错误: 文件夹不存在 {self.json_folder}")
            return False
        
        # 确认操作
        print("⚠️  警告: 此操作将把 JSON 数据导入到数据库中")
        print(f"📂 源文件夹: {self.json_folder}")
        print(f"💾 目标数据库: {db.DB_FILE}")
        print()
        
        confirm = input("确认继续? [y/N]: ").strip().lower()
        if confirm not in ['y', 'yes']:
            print("❌ 操作已取消")
            return False
        
        print()
        
        # 开始迁移
        start_time = datetime.now()
        
        # 1. 迁移 Bot 配置
        self.migrate_bots()
        
        # 2. 迁移消息映射
        self.migrate_mappings()
        
        # 3. 迁移已验证用户
        self.migrate_verified_users()
        
        # 4. 迁移黑名单
        self.migrate_blacklist()
        
        # 完成
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*60)
        print("🎉 迁移完成！")
        print("="*60)
        print(f"\n📊 迁移统计:")
        print(f"  • Bot 配置: {self.stats['bots']} 个")
        print(f"  • 消息映射: {self.stats['mappings']} 条")
        print(f"  • 验证用户: {self.stats['verified_users']} 个")
        print(f"  • 黑名单: {self.stats['blacklist']} 个")
        print(f"  • 耗时: {duration:.2f} 秒")
        
        if self.stats['errors']:
            print(f"\n⚠️  错误数量: {len(self.stats['errors'])} 个")
            print("\n错误详情:")
            for error in self.stats['errors'][:10]:  # 只显示前10个错误
                print(f"  - {error}")
            if len(self.stats['errors']) > 10:
                print(f"  ... 还有 {len(self.stats['errors']) - 10} 个错误未显示")
        
        print("\n✅ 迁移完成！现在可以删除旧的 JSON 文件了")
        print()
        
        return True


def main():
    """主函数"""
    print("""
╔═══════════════════════════════════════════════════════╗
║                                                       ║
║     Telegram Bot JSON → SQLite 迁移工具               ║
║                                                       ║
║     将旧的 JSON 文件数据迁移到新的 SQLite 数据库       ║
║                                                       ║
╚═══════════════════════════════════════════════════════╝
    """)
    
    # 获取 JSON 文件夹路径
    if len(sys.argv) > 1:
        json_folder = sys.argv[1]
    else:
        print("💡 提示: 可以指定 JSON 文件夹路径作为参数")
        print("   示例: python3 migrate_json_to_db.py /path/to/json_folder")
        print()
        json_folder = input("📂 请输入 JSON 文件夹路径 (回车使用当前目录): ").strip()
        
        if not json_folder:
            json_folder = os.getcwd()
    
    # 展开用户路径（~）
    json_folder = os.path.expanduser(json_folder)
    
    # 创建迁移工具并执行
    migrator = JSONMigrator(json_folder)
    success = migrator.run()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
