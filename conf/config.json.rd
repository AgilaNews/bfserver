{
    "log": {
        "path": "/home/work/logs/comment.log",
        "level": "DEBUG",
        "console": true
    },
    "redis": {
        "addr": "127.0.0.1:6379"
    },
    "mysql": {
        "read": {
                "host": "127.0.0.1",
                "port": 3306,
                "db": "comment",
                "user": "root",
                "password": "MhxzKhl-Happy!@#",
                "pool": 30
                },              
        "write": {
                "host": "127.0.0.1",
                "port": 3306,   
                "db": "comment",
                "user": "root",
                "password": "MhxzKhl-Happy!@#",
                "pool": 10
                }
    },
    "rpc": {
        "comment": {
                   "addr": ":6087"
        },
        "callbacks" : [
                    {
                        "product": "agilanews",
                        "timeout": 3000,
                        "addr": ":6098"
                    },
                    {
                        "product": "twhighlights",
                        "timeout": 3000,
                        "addr": ":6098"
                    }
              ]
    }
}
