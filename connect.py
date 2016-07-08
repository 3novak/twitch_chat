
import re
import socket
import time

# functions used to connect to twitch chat
def send_pong(irc, msg):
    irc.send(bytes('PONG %s\r\n' % msg, 'UTF-8'))

def send_nick(irc, nick):
    irc.send(bytes('NICK %s\r\n' % nick, 'UTF-8'))

def send_pass(irc, password):
    irc.send(bytes('PASS %s\r\n' % password, 'UTF-8'))

def channel_join(irc, chan):
    irc.send(bytes('JOIN %s\r\n' % chan, 'UTF-8'))

def channel_part(irc, chan):
    irc.send(bytes('PART %s\r\n' % chan, 'UTF-8'))

# initialize the info to be passed via the connection
def init(HOST='irc.twitch.tv',
         PORT=6667,
         CHAN='#trick2g',
         NICK='novakainerx',
         PASS='oauth:66pehrgkrt1l5c5z1wbaszejzw1znf'):
    HOST = HOST
    PORT = PORT
    CHAN = CHAN
    NICK = NICK
    PASS = PASS

    return {'HOST': HOST, 'PORT': PORT, 'CHAN': CHAN, 'NICK': NICK, 'PASS': PASS}

def get_sender(msg):
    result = ''
    for char in msg:
        if char == '!':
            break
        if char != ':':
            result += char
    return result


def get_message(msg):
    result = ''
    i = 3
    length = len(msg)
    while i < length:
        result += msg[i] + ' '
        i += 1
    result = result.lstrip(':')
    return result


def parse_message(msg):
    if len(msg) >= 1:
        msg = msg.split(' ')

def collect_data(file_dest):
    with open(file_dest, 'a') as f:
        data = ''
        data_list = ''
        msg_num = 0
        while True:
            try:
                data = data + irc.recv(1024).decode('UTF-8', errors='ignore')
                data_split = re.split(r'[~\r\n]+', data)
                data = data_split.pop()

                for line in data_split:
                    line = str.rstrip(line)
                    line = str.split(line)

                    if len(line) >= 1:
                        if line[0] == 'PING':
                            send_pong(irc, line[1])
                        try:
                            if line[1] == 'PRIVMSG':
                                sender = get_sender(line[0])
                                message = get_message(line)
                                parse_message(message)

                                tmp = time.strftime('%Y%m%d`|=%H:%M:%S`|=') + sender + '`|=' + message + '\n'
                                print(sender + ': ' + message, str(msg_num))

                                data_list += tmp
                                msg_num += 1

                                if msg_num == 100:
                                    print('\nwriting to file...\n')
                                    f.write(data_list)
                                    msg_num = 0
                        except:
                            print('we got a problem:\n')

            except socket.error:
                print("Socket died")

            except socket.timeout:
                print("Socket timeout")


if __name__ == '__main__':

    call_dict = init(CHAN='#voyboy')

    irc = socket.socket()
    irc.connect((call_dict['HOST'], call_dict['PORT']))

    send_pass(irc, call_dict['PASS'])
    send_nick(irc, call_dict['NICK'])
    channel_join(irc, call_dict['CHAN'])

    collect_data(file_dest=call_dict['CHAN'][1:] + '.txt')
