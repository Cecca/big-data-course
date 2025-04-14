#!/usr/bin/env python

from passlib.hash import sha512_crypt
import sys

if len(sys.argv) != 2:
    print("USAGE: python create_groupsdata.py NUM_GROUPS", file=sys.stderr)
    sys.exit(1)

num_users = int(sys.argv[1])

for user_num in range(1, num_users + 1):
    user_id = "group{:03d}".format(user_num)
    passwd = "{}pwd".format(user_id)
    crypt = sha512_crypt.using(rounds=5000).hash(passwd)
    print(
        """\
  - name: {}
    password: {}""".format(user_id, crypt)
    )
