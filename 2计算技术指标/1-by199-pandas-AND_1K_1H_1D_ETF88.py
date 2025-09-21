#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Redis地址池方式读取数据脚本
从192.168.102.199读取key: AND_1K_1H_1D_ETF88
"""

import redis
import pandas as pd
import io
import logging
import time
import paho.mqtt.client as mqtt
from typing import Optional, List
from redis.connection import ConnectionPool

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedisDataReader:
    """Redis数据读取器，使用连接池管理连接"""
    
    def __init__(self, host: str = '192.168.102.199', port: int = 6379, 
                 db: int = 0, password: Optional[str] = None, 
                 max_connections: int = 10):
        """
        初始化Redis连接池
        
        Args:
            host: Redis服务器地址
            port: Redis端口
            db: Redis数据库编号
            password: Redis密码
            max_connections: 最大连接数
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        
        # 创建连接池
        self.pool = ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
            max_connections=max_connections,
            decode_responses=True,  # 自动解码响应为字符串
            socket_connect_timeout=5,  # 连接超时
            socket_timeout=5,  # 读取超时
            retry_on_timeout=True,  # 超时重试
            health_check_interval=30  # 健康检查间隔
        )
        
        logger.info(f"Redis连接池已初始化 - 服务器: {host}:{port}, 数据库: {db}")
    
    def get_redis_client(self) -> redis.Redis:
        """获取Redis客户端连接"""
        return redis.Redis(connection_pool=self.pool)
    
    def read_from_redis(self, data_key: str) -> pd.DataFrame:
        """
        从Redis读取CSV格式的数据
        
        Args:
            data_key: Redis键名
            
        Returns:
            pandas.DataFrame: 解析后的数据框
        """
        try:
            redis_client = self.get_redis_client()
            
            # 检查键是否存在
            if not redis_client.exists(data_key):
                logger.warning(f"Redis键 '{data_key}' 不存在 (服务器: {self.host}:{self.port})")
                return pd.DataFrame()
            
            # 获取数据
            csv_data = redis_client.get(data_key)
            
            # 调试信息：检查Redis中是否有数据
            if csv_data is None:
                logger.warning(f"Redis键 '{data_key}' 中没有数据或值为空 (服务器: {self.host}:{self.port})")
                # 检查Redis中所有的键
                try:
                    all_keys = redis_client.keys('*')
                    logger.info(f"Redis中所有键: {all_keys}")
                except Exception as e:
                    logger.error(f"无法获取Redis键列表: {e}")
                return pd.DataFrame()
            
            logger.info(f"从Redis键 '{data_key}' 成功读取数据，数据长度: {len(csv_data)} (服务器: {self.host}:{self.port})")
            
            # 解析CSV数据
            df = pd.read_csv(io.StringIO(csv_data))
            logger.info(f"成功解析CSV数据，行数: {len(df)}，列数: {len(df.columns)}")
            
            return df
            
        except redis.RedisError as e:
            logger.error(f"Redis连接错误：{e}")
            return pd.DataFrame()
        except pd.errors.EmptyDataError:
            logger.error("CSV数据为空，无法解析")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"从Redis读取数据失败：{e}")
            return pd.DataFrame()
    
    def get_all_keys(self, pattern: str = '*') -> List[str]:
        """
        获取Redis中所有匹配的键
        
        Args:
            pattern: 键名模式，默认为'*'匹配所有
            
        Returns:
            List[str]: 匹配的键列表
        """
        try:
            redis_client = self.get_redis_client()
            keys = redis_client.keys(pattern)
            logger.info(f"找到 {len(keys)} 个匹配的键: {keys}")
            return keys
        except Exception as e:
            logger.error(f"获取Redis键列表失败：{e}")
            return []
    
    def test_connection(self) -> bool:
        """
        测试Redis连接
        
        Returns:
            bool: 连接是否成功
        """
        try:
            redis_client = self.get_redis_client()
            redis_client.ping()
            logger.info(f"Redis连接测试成功 (服务器: {self.host}:{self.port})")
            return True
        except Exception as e:
            logger.error(f"Redis连接测试失败：{e}")
            return False
    
    def close(self):
        """关闭连接池"""
        try:
            self.pool.disconnect()
            logger.info("Redis连接池已关闭")
        except Exception as e:
            logger.error(f"关闭Redis连接池失败：{e}")


def get_data_redis_client() -> redis.Redis:
    """
    获取Redis客户端连接（兼容原有代码）
    
    Returns:
        redis.Redis: Redis客户端实例
    """
    # 使用默认配置创建连接池
    pool = ConnectionPool(
        host='192.168.102.199',
        port=6379,
        db=0,
        decode_responses=True,
        max_connections=10
    )
    return redis.Redis(connection_pool=pool)


def read_from_redis(data_key: str) -> pd.DataFrame:
    """
    从Redis读取CSV格式的数据 (从192.168.102.199)
    
    Args:
        data_key: Redis键名
        
    Returns:
        pandas.DataFrame: 解析后的数据框
    """
    try:
        redis_client = get_data_redis_client()
        csv_data = redis_client.get(data_key)
        
        # 调试信息：检查Redis中是否有数据
        if csv_data is None:
            print(f"Redis键 '{data_key}' 中没有数据或值为空 (服务器: 192.168.102.199)")
            # 检查Redis中所有的键
            try:
                all_keys = redis_client.keys('*')
                print(f"Redis中所有键: {all_keys}")
            except:
                print("无法获取Redis键列表")
            return pd.DataFrame()
        
        print(f"从Redis键 '{data_key}' 成功读取数据，数据长度: {len(csv_data)} (服务器: 192.168.102.199)")
        
        df = pd.read_csv(io.StringIO(csv_data))
        print(f"成功解析CSV数据，行数: {len(df)}，列数: {len(df.columns)}")
        return df
    except Exception as e:
        print(f"从Redis读取数据失败：{e}")
        return pd.DataFrame()


def analyze_1d_z4l02_column(df: pd.DataFrame) -> dict:
    """
    统计1D_Z4L02列的值分布
    
    Args:
        df: 包含1D_Z4L02列的数据框
        
    Returns:
        dict: 包含各种统计结果的字典
    """
    if df.empty:
        print("数据框为空，无法进行统计")
        return {}
    
    if '1D_Z4L02' not in df.columns:
        print("数据框中没有找到'1D_Z4L02'列")
        print(f"可用列名: {list(df.columns)}")
        return {}
    
    # 获取1D_Z4L02列的数据
    column_data = df['1D_Z4L02']
    
    # 统计各种条件的数量
    stats = {}
    
    # 等于1的值的数量
    stats['1D_Z4L02_M1'] = (column_data == 1).sum()
    
    # 等于-1的值的数量
    stats['1D_Z4L02_MF1'] = (column_data == -1).sum()
    
    # 等于2的值的数量
    stats['1D_Z4L02_M2'] = (column_data == 2).sum()
    
    # 等于-2的值的数量
    stats['1D_Z4L02_MF2'] = (column_data == -2).sum()
    
    # 大于1的值的数量
    stats['1D_Z4L02_MD1'] = (column_data > 1).sum()
    
    # 小于-1的值的数量
    stats['1D_Z4L02_MXF1'] = (column_data < -1).sum()
    
    # 大于2的值的数量
    stats['1D_Z4L02_MD2'] = (column_data > 2).sum()
    
    # 小于-2的值的数量
    stats['1D_Z4L02_MXF2'] = (column_data < -2).sum()
    
    # 额外的统计信息
    stats['total_count'] = len(column_data)
    stats['null_count'] = column_data.isnull().sum()
    stats['unique_values'] = column_data.nunique()
    
    # 打印统计结果
    print("\n" + "=" * 50)
    print("1D_Z4L02列统计结果:")
    print("=" * 50)
    print(f"总数据量: {stats['total_count']}")
    print(f"空值数量: {stats['null_count']}")
    print(f"唯一值数量: {stats['unique_values']}")
    print(f"等于1的数量 (1D_Z4L02_M1): {stats['1D_Z4L02_M1']}")
    print(f"等于-1的数量 (1D_Z4L02_MF1): {stats['1D_Z4L02_MF1']}")
    print(f"等于2的数量 (1D_Z4L02_M2): {stats['1D_Z4L02_M2']}")
    print(f"等于-2的数量 (1D_Z4L02_MF2): {stats['1D_Z4L02_MF2']}")
    print(f"大于1的数量 (1D_Z4L02_MD1): {stats['1D_Z4L02_MD1']}")
    print(f"小于-1的数量 (1D_Z4L02_MXF1): {stats['1D_Z4L02_MXF1']}")
    print(f"大于2的数量 (1D_Z4L02_MD2): {stats['1D_Z4L02_MD2']}")
    print(f"小于-2的数量 (1D_Z4L02_MXF2): {stats['1D_Z4L02_MXF2']}")
    
    # 显示值的分布
    print(f"\n值分布:")
    value_counts = column_data.value_counts().sort_index()
    print(value_counts)
    
    return stats


def analyze_1h_z4l02_column(df: pd.DataFrame) -> dict:
    """
    统计1H_Z4L02列的值分布
    
    Args:
        df: 包含1H_Z4L02列的数据框
        
    Returns:
        dict: 包含各种统计结果的字典
    """
    if df.empty:
        print("数据框为空，无法进行统计")
        return {}
    
    if '1H_Z4L02' not in df.columns:
        print("数据框中没有找到'1H_Z4L02'列")
        print(f"可用列名: {list(df.columns)}")
        return {}
    
    # 获取1H_Z4L02列的数据
    column_data = df['1H_Z4L02']
    
    # 统计各种条件的数量
    stats = {}
    
    # 等于1的值的数量
    stats['1H_Z4L02_M1'] = (column_data == 1).sum()
    
    # 等于-1的值的数量
    stats['1H_Z4L02_MF1'] = (column_data == -1).sum()
    
    # 等于2的值的数量
    stats['1H_Z4L02_M2'] = (column_data == 2).sum()
    
    # 等于-2的值的数量
    stats['1H_Z4L02_MF2'] = (column_data == -2).sum()
    
    # 大于1的值的数量
    stats['1H_Z4L02_MD1'] = (column_data > 1).sum()
    
    # 小于-1的值的数量
    stats['1H_Z4L02_MXF1'] = (column_data < -1).sum()
    
    # 大于2的值的数量
    stats['1H_Z4L02_MD2'] = (column_data > 2).sum()
    
    # 小于-2的值的数量
    stats['1H_Z4L02_MXF2'] = (column_data < -2).sum()
    
    # 额外的统计信息
    stats['total_count'] = len(column_data)
    stats['null_count'] = column_data.isnull().sum()
    stats['unique_values'] = column_data.nunique()
    
    # 打印统计结果
    print("\n" + "=" * 50)
    print("1H_Z4L02列统计结果:")
    print("=" * 50)
    print(f"总数据量: {stats['total_count']}")
    print(f"空值数量: {stats['null_count']}")
    print(f"唯一值数量: {stats['unique_values']}")
    print(f"等于1的数量 (1H_Z4L02_M1): {stats['1H_Z4L02_M1']}")
    print(f"等于-1的数量 (1H_Z4L02_MF1): {stats['1H_Z4L02_MF1']}")
    print(f"等于2的数量 (1H_Z4L02_M2): {stats['1H_Z4L02_M2']}")
    print(f"等于-2的数量 (1H_Z4L02_MF2): {stats['1H_Z4L02_MF2']}")
    print(f"大于1的数量 (1H_Z4L02_MD1): {stats['1H_Z4L02_MD1']}")
    print(f"小于-1的数量 (1H_Z4L02_MXF1): {stats['1H_Z4L02_MXF1']}")
    print(f"大于2的数量 (1H_Z4L02_MD2): {stats['1H_Z4L02_MD2']}")
    print(f"小于-2的数量 (1H_Z4L02_MXF2): {stats['1H_Z4L02_MXF2']}")
    
    # 显示值的分布
    print(f"\n值分布:")
    value_counts = column_data.value_counts().sort_index()
    print(value_counts)
    
    return stats


def analyze_1k_z4l02_column(df: pd.DataFrame) -> dict:
    """
    统计1K_Z4L02列的值分布
    
    Args:
        df: 包含1K_Z4L02列的数据框
        
    Returns:
        dict: 包含各种统计结果的字典
    """
    if df.empty:
        print("数据框为空，无法进行统计")
        return {}
    
    if '1K_Z4L02' not in df.columns:
        print("数据框中没有找到'1K_Z4L02'列")
        print(f"可用列名: {list(df.columns)}")
        print("⚠️ 跳过1K_Z4L02列的分析，使用默认值0")
        
        # 返回默认值，避免MQTT发送时出现警告
        return {
            '1K_Z4L02_M1': 0,
            '1K_Z4L02_MF1': 0,
            '1K_Z4L02_M2': 0,
            '1K_Z4L02_MF2': 0,
            '1K_Z4L02_MD1': 0,
            '1K_Z4L02_MXF1': 0,
            '1K_Z4L02_MD2': 0,
            '1K_Z4L02_MXF2': 0
        }
    
    # 获取1K_Z4L02列的数据
    column_data = df['1K_Z4L02']
    
    # 统计各种条件的数量
    m1_count = (column_data == 1).sum()
    mf1_count = (column_data == -1).sum()
    m2_count = (column_data == 2).sum()
    mf2_count = (column_data == -2).sum()
    md1_count = (column_data == 1.5).sum()
    mxf1_count = (column_data == -1.5).sum()
    md2_count = (column_data == 2.5).sum()
    mxf2_count = (column_data == -2.5).sum()
    
    # 构建结果字典
    result = {
        '1K_Z4L02_M1': m1_count,
        '1K_Z4L02_MF1': mf1_count,
        '1K_Z4L02_M2': m2_count,
        '1K_Z4L02_MF2': mf2_count,
        '1K_Z4L02_MD1': md1_count,
        '1K_Z4L02_MXF1': mxf1_count,
        '1K_Z4L02_MD2': md2_count,
        '1K_Z4L02_MXF2': mxf2_count
    }
    
    print(f"1K_Z4L02列统计结果:")
    print(f"  M1 (1): {m1_count}")
    print(f"  MF1 (-1): {mf1_count}")
    print(f"  M2 (2): {m2_count}")
    print(f"  MF2 (-2): {mf2_count}")
    print(f"  MD1 (1.5): {md1_count}")
    print(f"  MXF1 (-1.5): {mxf1_count}")
    print(f"  MD2 (2.5): {md2_count}")
    print(f"  MXF2 (-2.5): {mxf2_count}")
    
    return result


def analyze_1w_z4l02_column(df: pd.DataFrame) -> dict:
    """
    统计1W_Z4L02列的值分布
    
    Args:
        df: 包含1W_Z4L02列的数据框
        
    Returns:
        dict: 包含各种统计结果的字典
    """
    if df.empty:
        print("数据框为空，无法进行统计")
        return {}
    
    if '1W_Z4L02' not in df.columns:
        print("数据框中没有找到'1W_Z4L02'列")
        print(f"可用列名: {list(df.columns)}")
        print("⚠️ 跳过1W_Z4L02列的分析，使用默认值0")
        
        # 返回默认值，避免MQTT发送时出现警告
        return {
            '1W_Z4L02_M1': 0,
            '1W_Z4L02_MF1': 0,
            '1W_Z4L02_M2': 0,
            '1W_Z4L02_MF2': 0,
            '1W_Z4L02_MD1': 0,
            '1W_Z4L02_MXF1': 0,
            '1W_Z4L02_MD2': 0,
            '1W_Z4L02_MXF2': 0
        }
    
    # 获取1W_Z4L02列的数据
    column_data = df['1W_Z4L02']
    
    # 统计各种条件的数量
    stats = {}
    
    # 等于1的值的数量
    stats['1W_Z4L02_M1'] = (column_data == 1).sum()
    
    # 等于-1的值的数量
    stats['1W_Z4L02_MF1'] = (column_data == -1).sum()
    
    # 等于2的值的数量
    stats['1W_Z4L02_M2'] = (column_data == 2).sum()
    
    # 等于-2的值的数量
    stats['1W_Z4L02_MF2'] = (column_data == -2).sum()
    
    # 大于1的值的数量
    stats['1W_Z4L02_MD1'] = (column_data > 1).sum()
    
    # 小于-1的值的数量
    stats['1W_Z4L02_MXF1'] = (column_data < -1).sum()
    
    # 大于2的值的数量
    stats['1W_Z4L02_MD2'] = (column_data > 2).sum()
    
    # 小于-2的值的数量
    stats['1W_Z4L02_MXF2'] = (column_data < -2).sum()
    
    # 额外的统计信息
    stats['total_count'] = len(column_data)
    stats['null_count'] = column_data.isnull().sum()
    stats['unique_values'] = column_data.nunique()
    
    # 打印统计结果
    print("\n" + "=" * 50)
    print("1W_Z4L02列统计结果:")
    print("=" * 50)
    print(f"总数据量: {stats['total_count']}")
    print(f"空值数量: {stats['null_count']}")
    print(f"唯一值数量: {stats['unique_values']}")
    print(f"等于1的数量 (1W_Z4L02_M1): {stats['1W_Z4L02_M1']}")
    print(f"等于-1的数量 (1W_Z4L02_MF1): {stats['1W_Z4L02_MF1']}")
    print(f"等于2的数量 (1W_Z4L02_M2): {stats['1W_Z4L02_M2']}")
    print(f"等于-2的数量 (1W_Z4L02_MF2): {stats['1W_Z4L02_MF2']}")
    print(f"大于1的数量 (1W_Z4L02_MD1): {stats['1W_Z4L02_MD1']}")
    print(f"小于-1的数量 (1W_Z4L02_MXF1): {stats['1W_Z4L02_MXF1']}")
    print(f"大于2的数量 (1W_Z4L02_MD2): {stats['1W_Z4L02_MD2']}")
    print(f"小于-2的数量 (1W_Z4L02_MXF2): {stats['1W_Z4L02_MXF2']}")
    
    # 显示值的分布
    print(f"\n值分布:")
    value_counts = column_data.value_counts().sort_index()
    print(value_counts)
    
    return stats


def main():
    """主函数 - 循环读取Redis和发送MQTT"""
    # 目标键名
    target_key = "AND_1K_1H_1D_ETF88"
    
    # 循环配置
    loop_interval = 5  # 循环间隔（秒）
    max_iterations = 0  # 最大循环次数，0表示无限循环
    
    print("=" * 60)
    print("Redis数据读取和MQTT发送脚本")
    print(f"目标键: {target_key}")
    print(f"服务器: 192.168.102.199:6379")
    print(f"循环间隔: {loop_interval}秒")
    print(f"最大循环次数: {'无限' if max_iterations == 0 else max_iterations}")
    print("=" * 60)
    
    # 创建Redis连接
    reader = RedisDataReader()
    
    # 测试连接
    if not reader.test_connection():
        print("❌ Redis连接失败，程序退出")
        return
    
    iteration_count = 0
    
    try:
        while True:
            iteration_count += 1
            print(f"\n{'='*60}")
            print(f"🔄 第 {iteration_count} 次循环 - {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")
            
            try:
                # 读取Redis数据
                print("📖 正在读取Redis数据...")
                df = reader.read_from_redis(target_key)
                
                if not df.empty:
                    print(f"✅ 数据读取成功: {len(df)} 行, {len(df.columns)} 列")
                    print(f"📋 数据列名: {list(df.columns)}")
                    
                    # 统计1D_Z4L02列
                    print("📊 正在分析1D_Z4L02列数据...")
                    stats_1d = analyze_1d_z4l02_column(df)
                    
                    # 统计1H_Z4L02列
                    print("📊 正在分析1H_Z4L02列数据...")
                    stats_1h = analyze_1h_z4l02_column(df)
                    
                    # 统计1K_Z4L02列
                    print("📊 正在分析1K_Z4L02列数据...")
                    stats_1k = analyze_1k_z4l02_column(df)
                    
                    # 统计1W_Z4L02列
                    print("📊 正在分析1W_Z4L02列数据...")
                    stats_1w = analyze_1w_z4l02_column(df)
                    
                    # 合并统计数据
                    all_stats = {}
                    if stats_1d:
                        all_stats.update(stats_1d)
                    if stats_1h:
                        all_stats.update(stats_1h)
                    if stats_1k:
                        all_stats.update(stats_1k)
                    if stats_1w:
                        all_stats.update(stats_1w)
                    
                    if all_stats:
                        print("📈 合并统计数据:")
                        for key, value in all_stats.items():
                            print(f"  {key} = {value}")
                        
                        # 发送数据到MQTT
                        print("📤 正在发送数据到MQTT...")
                        send_to_mqtt(all_stats)
                        print("✅ MQTT发送完成")
                    else:
                        print("⚠️ 统计数据为空")
                else:
                    print("⚠️ 未读取到数据")
                    
            except Exception as e:
                print(f"❌ 循环中发生错误: {e}")
                logger.error(f"循环错误: {e}")
            
            # 检查是否达到最大循环次数
            if max_iterations > 0 and iteration_count >= max_iterations:
                print(f"\n🏁 已达到最大循环次数 {max_iterations}，程序结束")
                break
            
            # 等待下次循环
            print(f"⏳ 等待 {loop_interval} 秒后进行下次循环...")
            print("💡 按 Ctrl+C 可以停止程序")
            time.sleep(loop_interval)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 程序被用户中断 (Ctrl+C)")
        print(f"📊 总共执行了 {iteration_count} 次循环")
    except Exception as e:
        print(f"\n❌ 程序发生严重错误: {e}")
        logger.error(f"程序错误: {e}")
    finally:
        # 关闭Redis连接
        reader.close()
        print("🔌 Redis连接已关闭")
        print("👋 程序结束")


def send_to_mqtt(stats_data, mqtt_host="192.168.102.16", mqtt_port=1883, 
                 mqtt_username="sh18", mqtt_password="grf123321"):
    """
    将统计数据发送到MQTT服务器
    
    Args:
        stats_data: 包含统计数据的字典
        mqtt_host: MQTT服务器地址
        mqtt_port: MQTT服务器端口
        mqtt_username: MQTT用户名
        mqtt_password: MQTT密码
    """
    try:
        # 创建MQTT客户端 (使用新的API)
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        client.username_pw_set(mqtt_username, mqtt_password)
        
        # 连接到MQTT服务器
        client.connect(mqtt_host, mqtt_port, 60)
        logger.info(f"已连接到MQTT服务器 {mqtt_host}:{mqtt_port}")
        
        # 要发送的变量列表
        variables_to_send = [
            # 1D_Z4L02列的统计变量
            '1D_Z4L02_M1', '1D_Z4L02_MF1', '1D_Z4L02_M2', '1D_Z4L02_MF2',
            '1D_Z4L02_MD1', '1D_Z4L02_MXF1', '1D_Z4L02_MD2', '1D_Z4L02_MXF2',
            # 1H_Z4L02列的统计变量
            '1H_Z4L02_M1', '1H_Z4L02_MF1', '1H_Z4L02_M2', '1H_Z4L02_MF2',
            '1H_Z4L02_MD1', '1H_Z4L02_MXF1', '1H_Z4L02_MD2', '1H_Z4L02_MXF2',
            # 1K_Z4L02列的统计变量
            '1K_Z4L02_M1', '1K_Z4L02_MF1', '1K_Z4L02_M2', '1K_Z4L02_MF2',
            '1K_Z4L02_MD1', '1K_Z4L02_MXF1', '1K_Z4L02_MD2', '1K_Z4L02_MXF2',
            # 1W_Z4L02列的统计变量
            '1W_Z4L02_M1', '1W_Z4L02_MF1', '1W_Z4L02_M2', '1W_Z4L02_MF2',
            '1W_Z4L02_MD1', '1W_Z4L02_MXF1', '1W_Z4L02_MD2', '1W_Z4L02_MXF2'
        ]
        
        print("\n" + "=" * 50)
        print("开始发送数据到MQTT服务器:")
        print("=" * 50)
        
        # 循环发送每个变量
        for var_name in variables_to_send:
            if var_name in stats_data:
                value = stats_data[var_name]
                # 发布消息，topic就是变量名，payload是值
                result = client.publish(var_name, str(value))
                
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    print(f"✓ 发送成功: {var_name} = {value}")
                    logger.info(f"MQTT发送成功: {var_name} = {value}")
                else:
                    print(f"✗ 发送失败: {var_name} = {value} (错误码: {result.rc})")
                    logger.error(f"MQTT发送失败: {var_name} = {value} (错误码: {result.rc})")
                
                # 短暂延迟，避免发送过快
                time.sleep(0.1)
            else:
                # 静默跳过不存在的变量，不显示警告
                pass
        
        # 断开连接
        client.disconnect()
        print("=" * 50)
        print("MQTT数据发送完成")
        print("=" * 50)
        logger.info("MQTT数据发送完成")
        
    except Exception as e:
        print(f"❌ MQTT发送过程中发生错误: {e}")
        logger.error(f"MQTT发送错误: {e}")


if __name__ == "__main__":
    main()
