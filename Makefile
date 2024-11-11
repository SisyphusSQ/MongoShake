all: 
	bash build.sh

linux: clean
	bash build.sh linux

clean:
	rm -rf bin
	rm -rf *.pprof
	rm -rf *.output
	rm -rf logs
	rm -rf diagnostic/
	rm -rf data/
	rm -rf *.pid
	rm -rf cmd/collector/diagnostic
	rm -rf cmd/receiver/diagnostic

darwin:
	bash build.sh darwin

darwin_test:
	bin/collector -version

darwin_run:
	bin/collector -conf conf/collector_sample.conf
