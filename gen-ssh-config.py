#!/usr/bin/env python

hosts = {}
with open('hosts.txt') as fp:
    for line in fp.readlines():
        host, ip = line.split()
        hosts[host] = ip

with open('frontend-public.txt') as fp:
    frontend_ip = fp.readline().strip()

print("""\
Host cloudveneto-frontend 
   HostName {}
   Port 2222
   User ubuntu
   IdentityFile ~/.ssh/cloudveneto-machines.pem
""".format(frontend_ip))

for host, ip in hosts.items():
    print("""\
Host {}
   HostName {}
   User ubuntu
   IdentityFile ~/.ssh/cloudveneto-machines.pem
   ProxyCommand ssh cloudveneto-frontend -N -W %h:%p 2> /dev/null
""".format(host, ip))
