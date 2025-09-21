from moomoo import *

#https://openapi.futunn.com/futu-api-doc/quote/get-kl.html

import time

import redis
import pandas as pd

redis_host1 = '127.0.0.1'

redis_host2 = '127.0.0.1'

x500 = 50


start_time0 = time.time()


# 创建一个连接池（这应该是全局的，或者在你的应用中只初始化一次）
redis_pool = redis.ConnectionPool(host=redis_host1, port=6379, db=0)

redis_pool52 = redis.ConnectionPool(host=redis_host2, port=6379, db=0)


def pandas_to_redis(hostip, key_name, dfx):
    # 使用连接池来获取连接
    r = redis.Redis(connection_pool=redis_pool)
    
    # 将 DataFrame 转换为逗号分隔的字符串
    df_str = dfx.to_csv(index=False)
    
    # 将DataFrame字符串存储到Redis中
    r.set(key_name, df_str)

    # 这里不再需要显式关闭连接，连接会自动被归还到连接池



#这个不是恒生科技 是恒生指数

'HK.00001', 'HK.00002', 'HK.00003', 'HK.00005', 'HK.00006', 'HK.00011', 'HK.00012', 'HK.00016', 'HK.00017', 'HK.00027', 'HK.00066', 'HK.00101', 'HK.00175', 'HK.00241', 'HK.00267', 'HK.00288', 'HK.00291', 'HK.00316', 'HK.00322', 'HK.00386', 'HK.00388', 'HK.00669', 'HK.00688', 'HK.00700', 'HK.00762', 'HK.00823', 'HK.00836', 'HK.00857', 'HK.00868', 'HK.00881', 'HK.00883', 'HK.00939', 'HK.00941', 'HK.00960', 'HK.00968', 'HK.00981', 'HK.00992', 'HK.01038', 'HK.01044', 'HK.01088', 'HK.01093', 'HK.01099', 'HK.01109', 'HK.01113', 'HK.01177', 'HK.01209', 'HK.01211', 'HK.01299', 'HK.01378', 'HK.01398', 'HK.01810', 'HK.01876', 'HK.01928', 'HK.01929', 'HK.01997', 'HK.02015', 'HK.02020', 'HK.02269', 'HK.02313', 'HK.02318', 'HK.02319', 'HK.02331', 'HK.02359', 'HK.02382', 'HK.02388', 'HK.02628', 'HK.02688', 'HK.02899', 'HK.03690', 'HK.03692', 'HK.03968', 'HK.03988', 'HK.06098', 'HK.06618', 'HK.06690', 'HK.06862', 'HK.09618', 'HK.09633', 'HK.09888', 'HK.09961', 'HK.09988', 'HK.09999'


#恒生科技

#codelist2 = ['HK.00020', 'HK.00241', 'HK.00268', 'HK.00285', 'HK.00700', 'HK.00772', 'HK.00981', 'HK.00992', 'HK.01024', 'HK.01347', 'HK.01797', 'HK.01810', 'HK.01833', 'HK.02015', 'HK.02382', 'HK.03690', 'HK.03888', 'HK.06060', 'HK.06618', 'HK.06690', 'HK.09618', 'HK.09626', 'HK.09698', 'HK.09866', 'HK.09868', 'HK.09888', 'HK.09898', 'HK.09961', 'HK.09988', 'HK.09999']

# 记录开始时间
#start_time = time.time()


#下面是 恒生和 科技合并
#codelist = ['HK.00268', 'HK.00001', 'HK.00762', 'HK.00017', 'HK.00012', 'HK.00291', 'HK.01024', 'HK.01378', 'HK.01833', 'HK.00669', 'HK.01797', 'HK.00868', 'HK.01088', 'HK.00005', 'HK.02269', 'HK.09866', 'HK.02331', 'HK.01347', 'HK.01299', 'HK.02319', 'HK.00175', 'HK.00066', 'HK.01211', 'HK.01997', 'HK.02359', 'HK.00772', 'HK.01929', 'HK.00857', 'HK.00016', 'HK.01876', 'HK.00386', 'HK.06690', 'HK.03968', 'HK.02313', 'HK.02015', 'HK.00941', 'HK.00883', 'HK.00011', 'HK.00992', 'HK.09898', 'HK.02318', 'HK.09999', 'HK.00006', 'HK.00027', 'HK.09633', 'HK.01177', 'HK.00002', 'HK.09868', 'HK.00241', 'HK.09698', 'HK.01398', 'HK.02688', 'HK.00960', 'HK.00700', 'HK.09988', 'HK.03988', 'HK.02899', 'HK.03692', 'HK.00003', 'HK.03888', 'HK.00939', 'HK.09961', 'HK.00288', 'HK.02382', 'HK.00881', 'HK.09626', 'HK.02020', 'HK.00981', 'HK.01038', 'HK.09618', 'HK.01109', 'HK.01810', 'HK.06862', 'HK.00020', 'HK.00285', 'HK.06618', 'HK.01928', 'HK.01209', 'HK.01113', 'HK.06060', 'HK.01099', 'HK.02628', 'HK.02388', 'HK.00836', 'HK.01044', 'HK.00101', 'HK.09888', 'HK.00968', 'HK.06098', 'HK.01093', 'HK.00688', 'HK.00267', 'HK.00823', 'HK.00322', 'HK.00388', 'HK.03690', 'HK.00316']

#codelist = ['515880.SH', '515980.SH', '515050.SH', '159363.SZ', '159819.SZ', '588780.SH', '512930.SH', '515070.SH', '588200.SH', '588760.SH', '588750.SH', '588930.SH', '588790.SH', '159780.SZ', '159783.SZ', '562950.SH', '159801.SZ', '159995.SZ', '159813.SZ', '516780.SH', '159713.SZ', '588080.SH', '512480.SH', '588060.SH', '588000.SH', '159949.SZ', '561980.SH', '159915.SZ', '516510.SH', '159890.SZ', '515000.SH', '561910.SH', '159967.SZ', '562800.SH', '159796.SZ', '159732.SZ', '561160.SH', '512400.SH', '159206.SZ', '159786.SZ', '588190.SH', '159851.SZ', '516860.SH', '515250.SH', '159218.SZ', '159840.SZ', '159755.SZ', '159852.SZ', '159998.SZ','512720.SH', '515790.SH', '516010.SH', '159869.SZ', '515030.SH', '562500.SH', '159770.SZ', '588830.SH', '159559.SZ', '159601.SZ', '159805.SZ', '510500.SH', '516320.SH', '159870.SZ', '159790.SZ', '512580.SH', '512050.SH', '159338.SZ', '512100.SH', '512680.SH', '512880.SH', '510300.SH', '159993.SZ', '512660.SH', '512690.SH', '932000.SH', '510180.SH', '159928.SZ', '563300.SH', '159227.SZ', '512070.SH', '159825.SZ', '515170.SH', '510210.SH', '159399.SZ', '513970.SH', '510050.SH', '512200.SH', '159883.SZ', '513360.SH', '512670.SH', '513160.SH', '512710.SH', '159766.SZ', '513090.SH', '513750.SH', '512170.SH', '560700.SH', '159740.SZ', '513180.SH', '513980.SH', '513380.SH', '159929.SZ', '159792.SZ', '513050.SH', '512010.SH', '513520.SH', '159992.SZ', '513060.SH', '513120.SH', '159509.SZ', '516970.SH', '159615.SZ', '159506.SZ', '159611.SZ', '518880.SH', '513660.SH', '159980.SZ', '513330.SH', '515210.SH', '512890.SH', '515080.SH', '515220.SH', '517180.SH', '159930.SZ', '159941.SZ', '510880.SH', '512800.SH', '159612.SZ', '159876.SZ', '512000.SH']




codelist = ['159994.SZ', '515050.SH', '512930.SH', '159786.SZ', '512480.SH', '561980.SH', '512980.SH', '159805.SZ', '159992.SZ', '159363.SZ', '159796.SZ', '159755.SZ', '159611.SZ', '512200.SH', '515210.SH', '516320.SH', '515790.SH', '512670.SH', '159227.SZ', '510880.SH', '512890.SH', '159870.SZ', '512580.SH', '518880.SH', '159322.SZ', '159559.SZ', '562500.SH', '516970.SH', '159998.SZ', '513360.SH', '159851.SZ', '516860.SH', '512690.SH', '512660.SH', '512680.SH', '512710.SH', '588790.SH', '588930.SH', '588750.SH', '588200.SH', '588290.SH', '588780.SH', '588830.SH', '515000.SH', '159840.SZ', '159766.SZ', '515220.SH', '159930.SZ', '159825.SZ', '512000.SH', '515070.SH', '159819.SZ', '515980.SH', '588760.SH', '159852.SZ', '515170.SH', '159780.SZ', '159790.SZ', '515880.SH', '159583.SZ', '159206.SZ', '159218.SZ', '516780.SH', '562800.SH', '159928.SZ', '159732.SZ', '562950.SH', '159995.SZ', '159801.SZ', '515030.SH', '560700.SH', '512170.SH', '159883.SZ', '512010.SH', '512800.SH', '516010.SH', '159869.SZ', '159980.SZ', '512400.SH', '159876.SZ', '516510.SH', '159738.SZ', '512880.SH', '159993.SZ', '512070.SH', '515250.SH', '517180.SH', '515080.SH']





#'159583.SZ','159988.SZ',



# 取前300个
#codelist = codelist[:50]


# 反转格式
codelist = [code.split('.')[1] + '.' + code.split('.')[0] for code in codelist]


print(codelist)

# 重命名列
new_column_names = {
    'code': 'Code',
    'name': 'Name',
    'time_key': 'DateTime',
    'open': 'Open',
    'close': 'Close',
    'high': 'High',
    'low': 'Low',
    'volume': 'Volume',
    'turnover': 'Turnover',
    'pe_ratio': 'PE_Ratio',
    'turnover_rate': 'Turnover_Rate',
    'last_close': 'Last_Close'
}

try: 
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    ret_sub, err_message = quote_ctx.subscribe(codelist, [SubType.K_60M], subscribe_push=False)
except Exception as e:
    print("111")
    print(str(e))

            
            

from datetime import datetime, timedelta


def is_time_between_8_and_1605():
    # 获取当前时间和星期几
    current_time = datetime.now().time()
    today_weekday = datetime.now().weekday()
    
    # 定义工作日和非工作日的时间范围
    if 0 <= today_weekday <= 6:  # 如果今天是工作日（周一到周五）
        start_time = current_time.replace(hour=8, minute=0, second=0, microsecond=0)
    else:  # 如果今天是非工作日（周六和周日）
        start_time = current_time.replace(hour=8, minute=0, second=0, microsecond=0)
    
    end_time = current_time.replace(hour=15, minute=0, second=0, microsecond=0)
    
    # 判断当前时间是否在设定的时间范围内
    return start_time <= current_time <= end_time






for i in codelist:
        
    try: 



        # 先订阅 K 线类型。订阅成功后 OpenD 将持续收到服务器的推送，False 代表暂时不需要推送给脚本
        if ret_sub == RET_OK:  # 订阅成功
            ret, data = quote_ctx.get_cur_kline(i, x500, KLType.K_60M, AuType.QFQ)  # 获取港股00700最近2个 K 线数据
            if ret == RET_OK:

                data = data.rename(columns=new_column_names)
                # 重新排序列

                new_column_order = ['DateTime', 'Close', 'High', 'Low', 'Open', 'Volume']
                data = data.reindex(columns=new_column_order)

                # 将 'DateTime' 列转换为 datetime 类型
                data['DateTime'] = pd.to_datetime(data['DateTime'])
                
                # 按 'DateTime' 列对 DataFrame 进行排序
                data = data.sort_values(by='DateTime', ascending=True)
                
                

                print(data[-3:])
                #print(type(data))
                
                try:
                
                    pandas_to_redis(redis_host1,'BY54_1H_'+i+'now_py1',data)
                    
                    print("上传 redis 服务完成")
                except Exception as e:
                    print("pandas_to_redis")
                    print(str(e))
                
                
                #print(data['turnover_rate'][0])   # 取第一条的换手率
                #print(data['turnover_rate'].values.tolist())   # 转为 list
            else:
                print('error:', data)
        else:
            print('subscription failed', err_message)
            
            
    except Exception as e:
        print("222")
        print(str(e))



    # 记录结束时间
    #end_time = time.time()

    #time.sleep(0.01)
    
quote_ctx.close()  # 关闭当条连接，OpenD 会在1分钟后自动取消相应股票相应类型的订阅
    
end_time = time.time()

# 计算并打印执行时间
elapsed_time = end_time - start_time0
print(f"代码执行耗时: {elapsed_time} 秒")


