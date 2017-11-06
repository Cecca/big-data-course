SHELL := /bin/bash
DATA=$(shell base64 -w0 minion-setup.sh)

master-setup.sh: master-setup.sh.template minion-setup.sh
	source credentials/ec2rc.sh && source elastiq-conf.sh && USER_DATA=$(DATA) utils/templater.sh master-setup.sh.template > master-setup.sh

.PHONY: clean
clean:
	$(RM) master-setup.sh
