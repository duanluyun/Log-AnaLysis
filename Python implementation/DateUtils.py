from datetime import datetime

def getTimeStr(time):
    try:
        time_str=time[time.index('[')+1:time.index(' ')]
        return time_str
    except:
        return '0l'


def reformat(time):
    time_str=getTimeStr(time)
    time=datetime.strptime(time_str,'%d/%b/%Y:%H:%M:%S')
    time=time.strftime('%Y-%m-%d %H:%M:%S')
    return time


