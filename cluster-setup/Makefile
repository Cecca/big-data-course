SHELL := /bin/bash
DATA=$(shell base64 -w0 minion-setup.sh)
CONF_FILE=configuration.sh
TEMPLATER=utils/templater.sh

default: bootstrap.sh

bootstrap.sh: master-setup.sh bootstrap.sh.template
	COMPRESSED_DATA=$(shell cat master-setup.sh | gzip | base64 -w0) $(TEMPLATER) bootstrap.sh.template > bootstrap.sh

minion-setup.sh: minion-setup.sh.template start-spark-cluster.sh $(CONF_FILE)
	source $(CONF_FILE) && $(TEMPLATER) minion-setup.sh.template > minion-setup.sh

master-setup.sh: master-setup.sh.template minion-setup.sh $(CONF_FILE)
	source credentials/ec2rc.sh && source $(CONF_FILE) && USER_DATA=$(DATA) $(TEMPLATER) master-setup.sh.template > master-setup.sh

.PHONY: clean
clean:
	$(RM) minion-setup.sh
	$(RM) master-setup.sh
