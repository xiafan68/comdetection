# -*- coding: utf-8 -*-  
#!/usr/bin/python   
import paramiko  
import threading  

class RedisClusterMonitor(object):
    def __init__(self, username, passwd, ips):
        self.username = username
        self.password = passwd
        self.ips = ips
        
    def monitor(self):  
        for ip in self.ips:
            try:  
                ssh = paramiko.SSHClient()  
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  
                ssh.connect(ip, 22, self.username, self.password, timeout=5)
                stdin, stdout, stderr = ssh.exec_command("ps aux|grep redis-server|awk '{print $2}'")  
    #           stdin.write("Y")   #简单交互，输入 ‘Y’   
                out = stdout.readlines() 
                if len(out) < 3:
                    print "start redis on server:%s" % (ip)
                    ssh.exec_command("cd /home/admin/xiafan/redis-2.8.19 && nohup src/redis-server redis.conf") 
                else:
                    # 屏幕输出
                    print "reply from server:%s" % (ip)  
                    for o in out:  
                        print o,
                print '%s\tOK\n' % (ip)  
                ssh.close()  
            except :  
                print '%s\tError\n' % (ip) 
                 
if __name__ == '__main__':  
    ips = ["10.11.1.51", "10.11.1.52", "10.11.1.53", "10.11.1.54"
            , "10.11.1.55", "10.11.1.56", "10.11.1.57", "10.11.1.58" , "10.11.1.61",
            "10.11.1.62" , "10.11.1.63"]
    username = "admin"  # 用户名  
    passwd = "Hadoop123"  # 密码  
    monitor = RedisClusterMonitor(username, passwd, ips)
    a = threading.Thread(target=monitor.monitor())   
    a.start() 
