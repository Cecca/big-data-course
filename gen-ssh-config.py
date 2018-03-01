#!/usr/bin/env python

IPS = [
    '10.67.41.152',
    '10.67.41.153',
    '10.67.41.156',
    '10.67.41.157',
    '10.67.41.158',
    '10.67.41.159',
    '10.67.41.160',
    '10.67.41.161',
    '10.67.41.162'
]

for ip, identifier in zip(IPS, range(1, len(IPS)+1)):
    print("""\
Host minion-{}
   HostName {}
   User ubuntu
   IdentityFile ~/.ssh/cloudveneto-machines.pem
   ProxyCommand ssh cloud-gate -N -W %h:%p 2> /dev/null
""".format(identifier, ip))
