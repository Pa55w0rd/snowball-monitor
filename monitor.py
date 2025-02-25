import pysnowball as ball  
from datetime import datetime, timedelta, time as dtime  
from apscheduler.schedulers.background import BackgroundScheduler  
import configparser  
import requests  
import json  
import os  
import pytz  
import signal  
import sys  
import random  

# 读取配置文件  
config = configparser.ConfigParser()  
config.read("config.ini")  

cube_ids = config.get('default', 'cube_ids').split(',')  
dingtalk_webhook = config.get('default', 'dingtalk_webhook')  
xq_a_token = config.get('default', 'xq_a_token')  
u = config.get('default', 'u')  
interval_type = config.get('default', 'interval_type')  
interval_value = config.getfloat('default', 'interval_value')  
at_mobiles = config.get('default', 'at_mobiles').split(',')  

processed_ids_file = "processed_ids.json"  

# 加载已经处理的ID  
def load_processed_ids():  
    if os.path.exists(processed_ids_file):  
        try:  
            with open(processed_ids_file, 'r') as f:  
                data = json.load(f)  
                if isinstance(data, list):  # 确保数据是一个列表  
                    return set(data)  
                else:  
                    print("文件内容不是列表，将被重置为空集合")  
                    return set()  
        except json.JSONDecodeError as e:  
            print(f"文件内容无法解析为 JSON: {e}, 将重置为空集合")  
            return set()  
        except Exception as e:  
            print(f"加载文件时发生错误: {e}, 将重置为空集合")  
            return set()  
    return set()  

processed_ids = load_processed_ids()  

ball.set_token(f"xq_a_token={xq_a_token};u={u}")  

def format_timestamp_with_timezone_adjustment(timestamp, hours=0):  
    dt_obj = datetime.utcfromtimestamp(timestamp / 1000).replace(tzinfo=pytz.UTC)  
    dt_obj = dt_obj + timedelta(hours=hours)  
    return dt_obj.astimezone(pytz.timezone('Asia/Shanghai')).strftime('%Y.%m.%d %H:%M:%S')  

def send_dingtalk_message(content):  
    headers = {  
        'Content-Type': 'application/json'  
    }  
    at_section = {"atMobiles": at_mobiles, "isAtAll": False}  
    data = {  
        "msgtype": "text",  
        "text": {  
            "content": content  
        },  
        "at": at_section  
    }  
    try:  
        response = requests.post(dingtalk_webhook, headers=headers, json=data)  
        if response.status_code == 200:  
            print("钉钉消息发送成功")  
        else:  
            print(f"钉钉消息发送失败，状态码: {response.status_code}")  
    except Exception as e:  
        print(f"发送钉钉消息时出错: {e}")  

def save_processed_ids():  
    try:  
        with open(processed_ids_file, 'w') as f:  
            json.dump(list(processed_ids), f)  
        print("已处理ID保存成功")  
    except Exception as e:  
        print(f"保存已处理ID时出错: {e}")  

def monitor_rebalancing_operations():  
    for cube_id in cube_ids:  
        try:  
            # 获取组合的详细信息  
            quote_response = ball.quote_current(cube_id)  
            quote_info = quote_response.get(cube_id, {})  
            name = quote_info.get("name", "未知名称")  
            
            # 获取最新调仓信息  
            rebalancing_response = ball.rebalancing_current(cube_id)  
            last_rb = rebalancing_response.get('last_rb')  

            if last_rb and last_rb.get('id') not in processed_ids:  
                # 如果这一条last_rb还没有处理过，则处理它  
                content = f"检测到新调仓操作，组合ID: {cube_id}\n"  
                content += f"组合名称: {name}\n"  
                content += f"  最新的一次调仓:\n"  
                content += f"    调仓ID: {last_rb.get('id')}\n"  
                content += f"    调仓状态: {last_rb.get('status')}\n"  
                created_at = format_timestamp_with_timezone_adjustment(last_rb.get('created_at'))  
                content += f"    调仓时间: {created_at}\n"  

                rebalancing_id = last_rb.get('id')  
                processed_ids.add(rebalancing_id)  # 将此调仓ID标记为已处理  

                # 根据rebalancing_id查询调仓历史  
                history_response = ball.rebalancing_history(cube_id, 5, 1)  
                history_list = history_response.get('list', [])  

                for history_item in history_list:  
                    if history_item.get('id') == rebalancing_id:  
                        rebalancing_histories = history_item.get('rebalancing_histories', [])  
                        for record in rebalancing_histories:  
                            stock_name = record.get('stock_name')  
                            stock_symbol = record.get('stock_symbol')  
                            prev_weight = record.get('prev_weight', 0)  # 使用0作为默认值  
                            price = record.get('price')  
                            weight = record.get('weight')  
                            content += f"    股票信息: {stock_name} ({stock_symbol})\n"  
                            content += f"    调仓价格: {price}\n"  
                            content += f"    调仓结果: {prev_weight}% -> {weight}%\n"  

                # 发送钉钉消息  
                send_dingtalk_message(content)  
                print(content)  # 控制台输出结果  

                # 持久化处理过的ID  
                save_processed_ids()  
        except Exception as e:  
            print(f"监控组合ID {cube_id} 时发生错误: {e}")  

def is_in_trading_hours():  
    now = datetime.now(pytz.timezone('Asia/Shanghai'))  
    morning_start = dtime(9, 25)  
    morning_end = dtime(11, 30)  
    afternoon_start = dtime(13, 0)  
    afternoon_end = dtime(15, 0)  

    return (morning_start <= now.time() <= morning_end) or (afternoon_start <= now.time() <= afternoon_end)  

def job():  
    t = convert_interval_to_str(interval_type, interval_value)  
    if is_in_trading_hours():  
        print(f"正在查询中。。。 当前时间 {datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y.%m.%d %H:%M:%S')} , 下一次执行 {t} 后")   
        monitor_rebalancing_operations()  
    else:  
        print(f"当前时间 {datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y.%m.%d %H:%M:%S')} 不在交易时间内, 下一次执行 {t} 后")  

def signal_handler(sig, frame):  
    print('程序中断。')  
    scheduler.shutdown()  
    sys.exit(0)  

def convert_interval_to_str(interval_type, interval_value):  
    if interval_type == 'seconds':  
        return f"{int(interval_value)}秒"  
    elif interval_type == 'minutes':  
        return f"{int(interval_value)}分"  
    elif interval_type == 'hours':  
        return f"{int(interval_value)}小时"  
    else:  
        return f"{int(interval_value)}分"  # 默认情况下  

# 注册信号处理程序  
signal.signal(signal.SIGINT, signal_handler)  
signal.signal(signal.SIGTERM, signal_handler)  

scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Shanghai'))  

# 将配置文件中的时间间隔类型映射到apscheduler的参数键  
interval_mapping = {  
    'seconds': 'seconds',  
    'minutes': 'minutes',  
    'hours': 'hours'  
}  

interval_key = interval_mapping.get(interval_type, 'minutes')  # 默认为minutes  

# 设置任务在交易时间内执行  
scheduler.add_job(job, 'interval', **{interval_key: interval_value})  

print("开始监控...")  

# 启动调度器之前立即执行一次查询  
job()  

scheduler.start()  

# 保持主线程运行  
try:  
    while True:  
        pass  
except (KeyboardInterrupt, SystemExit):  
    signal_handler(None, None)