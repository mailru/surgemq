CURRENT_DIR=$(shell pwd)

dep:
	dep init
	dep ensure

tests-run:
	go test -v \
		$(CURRENT_DIR)/topics \
		$(CURRENT_DIR)/sessions \
		$(CURRENT_DIR)/message \
		$(CURRENT_DIR)/auth \
		$(CURRENT_DIR)/service