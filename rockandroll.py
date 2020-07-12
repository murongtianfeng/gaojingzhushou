import json
import pika
import itchat
import random
import _thread
import time
import pandas as pd
import threading
import nfvgaojingkeshi as nfvgjks

alarm_city_list = []
alarm_id_list = []



def getgoupid():
    groups = itchat.get_chatrooms(update=True)
    for g in groups:
        #print(g['NickName'])
        if g['NickName'] == u"紧急事件处理组":
            to_group = g['UserName']
        
    return to_group



# 为线程定义一个函数
def print_time( threadName, qunid):
    while 1:
        try:
            itchat.send(str(random.randint(0,30)), 'filehelper') #这个filehelper就是微信上的文件传输助手
            time.sleep(60*10)
        except:
            print('消息发送失败，请检查微信是否掉线！')


def iscontainm(str1, str2):
	#str1 包含 str2
	lenth1 = len(str1)
	lenth2 = len(str2)
	
	if lenth1 < lenth2:
		return False
	
	zimushu = 0
	for i in range(lenth2):
		if list(str2)[i] in list(str1):
			zimushu += 1
	if zimushu == lenth2:
		return True
	else:
		return False

def cleanlist():
    while True:
        time.sleep(60*20)
        # 获取锁，用于线程同步
        threadLock.acquire()
        alarm_city_list.clear()
        alarm_id_list.clear()
        # 释放锁，开启下一个线程
        threadLock.release()
        print('清空一次')

def isinlist(alarm_city, alarm_id):
    #alarm_city 为空
    if alarm_city == '':
        # 获取锁，用于线程同步
        threadLock.acquire()
        for i in range(len(alarm_id_list)):
            if alarm_id_list[i] == alarm_id:
                # 释放锁，开启下一个线程
                threadLock.release()
                return True
        
        alarm_city_list.append('')
        alarm_id_list.append(alarm_id)
        # 释放锁，开启下一个线程
        threadLock.release()
        return False
    else:
        #alarm_city 不为空
        # 获取锁，用于线程同步
        threadLock.acquire()
        for j in range(len(alarm_id_list)):
            if alarm_city_list[j] == alarm_city and alarm_id_list[j] == alarm_id:
                # 释放锁，开启下一个线程
                threadLock.release()
                return True
        alarm_city_list.append(alarm_city)
        alarm_id_list.append(alarm_id)
        # 释放锁，开启下一个线程
        threadLock.release()
        return False
    
    
#读取告警ID
df = pd.read_excel('d:/gaojingid.xlsx')
alarm_list = list(df['gaojingid'])
print('重要告警：', len(alarm_list), '条')

nfv_df = pd.read_excel('d:/nfvgaojingid.xlsx')
nfv_alarm_list = list(nfv_df['gaojingid'])
print('nfv告警：', len(nfv_alarm_list), '条')

#登录微信
itchat.auto_login()
#获取群id
groupid = getgoupid()

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='10.217.6.217', credentials=pika.PlainCredentials('test', 'czk10101'))
)

channel = connection.channel()

# channel.queue_declare(queue='all-alarms', arguments={"x-message-ttl": 600000})


def callback(ch, method, properties, body):
    #print(" [x] Received %r" % json.loads(body,encoding='utf8'))
    #alarm_context 的类型为“字典”
    alarm_context = json.loads(body,encoding='utf8')
    #告警清除
    if alarm_context.__contains__('AlarmClearTime'):
        #告警清除消息
        #print(alarm_context)
        pass
    elif alarm_context['LocateNeStatus'] == 0:
        #全量告警消息
        if alarm_context['NmsAlarmId'] == '0403-007-033-10-000057' and alarm_context['AlarmRegion'] == '河南省' and alarm_context['AlarmSeverity'] in [1, 2]:
            # mut_los
            if not isinlist(alarm_context['AlarmRegion'], alarm_context['NmsAlarmId']):
                #5分钟内同一告警不再重复提醒
                sendmsg = '请注意!  ' + str(alarm_context['AlarmRegion']) + '  ' + str(alarm_context['Specialty']) +'  ' + str(alarm_context['AlarmTitle']) + '  ，请立即处理！'
                try:
                    itchat.send(sendmsg, groupid)
                    time.sleep(1)
                except:
                    print('消息发送失败，请检查微信是否掉线！')
                print(alarm_context['AlarmTitle'], '######', alarm_context['DiscoverTime'], '######', alarm_context['LocateNeStatus'], '######', alarm_context['NmsAlarmId'])
        #如果有重大故障告警，则发送微信提醒
        for i in range(len(alarm_list)):
            if alarm_context['NmsAlarmId'] == alarm_list[i].strip() and alarm_context['AlarmSeverity'] in [1, 2]:
                if not isinlist(alarm_context['AlarmRegion'], alarm_context['NmsAlarmId']):
                    #5分钟内同一告警不再重复提醒
                    sendmsg = '请注意!  ' + str(alarm_context['AlarmRegion']) + '  ' + str(alarm_context['Specialty']) + '  ' + str(alarm_context['AlarmTitle']) + '  ，请立即处理！'
                    try:
                        itchat.send(sendmsg, groupid)
                        time.sleep(1)
                    except:
                        print('消息发送失败，请检查微信是否掉线！')
                    print(alarm_context['AlarmTitle'], '######', alarm_context['DiscoverTime'], '######', alarm_context['LocateNeStatus'], '######', alarm_list[i])
                break
               
        # NFV告警
        for j in range(len(nfv_alarm_list)):
            if alarm_context['NmsAlarmId'] == nfv_alarm_list[j].strip() and alarm_context['SystemName'] in ['APP-HZZZhzqNFVO1AHW-03AHW010','APP-HZZZhzqNFVO1AER-01AER010']:
                if not isinlist(alarm_context['AlarmRegion'], alarm_context['NmsAlarmId']):
                    #5分钟内同一告警不再重复提醒
                    #获取科室名称
                    ksmc = nfvgjks.towhom(alarm_context['EquipmentClass'], alarm_context['NeName'])
                    sendmsg = '网络云告警  所属科室:' + ksmc + '设备类型:' + str(alarm_context['EquipmentClass']) + '  网元名称:' + str(alarm_context['NeName']) + '  告警标题:' + str(alarm_context['AlarmTitle']) +'  告警正文:'+ str(alarm_context['AlarmText'])
                    try:
                        itchat.send(sendmsg, groupid)
                        time.sleep(1)
                    except:
                        print('消息发送失败，请检查微信是否掉线！')
                    print(alarm_context['EquipmentClass'], '######', alarm_context['NeName'], '######', alarm_context['AlarmTitle'])
                break
        '''
        #输出告警
        if alarm_context['AlarmSeverity'] in [1, 2] and alarm_context['Specialty'] in ['无线接入网']:
            #1,2级告警
            print('#地市#', alarm_context['AlarmRegion'],
                  '#专业#', alarm_context['Specialty'], 
                  '#告警标题#', alarm_context['AlarmTitle'], 
                  '#标准化名称#', alarm_context['StandardAlarmName'],
                  '#网管告警ID#', alarm_context['NmsAlarmId'], 
                  '#机房#', alarm_context['MachineroomIDofZGTT'],
                  '#告警级别#', alarm_context['AlarmSeverity'])
        '''

channel.basic_consume(queue='all-alarms', on_message_callback=callback, auto_ack=True)

#print(' [*] Waiting for messages. To exit press CTRL+C')
#print('地市', '专业', '告警标题','标准化名称', '网管告警ID', '机房', '告警级别')
print('程序正在运行!')
_thread.start_new_thread( print_time, ("Thread-1", 2, ) )
threadLock = threading.Lock()
threading.Thread(target=cleanlist).start()
channel.start_consuming()
