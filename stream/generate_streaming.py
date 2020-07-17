# 持续产生流数据
import csv
import time
import os
import random
import datetime

# 数据的含义......
# 用户 商品id 商品数量 时间戳
# 用户xxx在xxx时间购买了id为xxx的商品xxxx个
def streaming_generate():
    result_file = os.getcwd() + os.sep + "input/streaming_data.csv"
    print(result_file)
    while True:
        with open(result_file, 'a') as f:
            writer = csv.writer(f, delimiter=',')
            time.sleep(random.randint(1, 30)) # 时间间隔随机值1~30s
            field1 = random.choice(["a", "b", "c", "d", "e"]) # 5个用户
            field2 = random.randint(1, 3) # 3中商品
            field3 = random.randint(4, 10) # 每次购买4~10件
            field4 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            line = "%s,%d,%d,%s" %(field1,field2,field3,field4)
            print(line)
            writer.writerow((field1,field2,field3,field4))
        f.close()
    return result_file  

if __name__ == "__main__":
    streaming_generate()

