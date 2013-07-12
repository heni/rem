from client import remclient


def clean_rem(url):
    connection = remclient.Connector(url)
    for q in connection.ListObjects('queues'):
        print q
        for p in connection.Queue(q[0]).ListPackets('all'):
            p.Stop()
            p.Delete()
        connection.Queue(q[0]).Delete()
