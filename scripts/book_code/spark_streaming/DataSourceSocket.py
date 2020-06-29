"""
socket数据源,创建一个socket服务器端,发送消息给客户端(NetworkWordCount程序)
"""

import socket

# 实例化socket对象
server = socket.socket()
# 绑定ip和端口
server.bind(('localhost', 9999))
# 监听绑定的端口
server.listen(1)  # 先准备n个挂起的连接,用不用,先放那,用的时候直接取即可
while 1:
    print('Waiting for connect...')
    conn, addr = server.accept()
    print('Connect success! Connection is from %s ' % addr[0])
    print('Sending data...')
    conn.send('I love spark I love pandas I love little panda'.encode())
    conn.close()
    print('Connection is broken.')

