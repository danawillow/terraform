go test ./terraform -timeout=10s | grep -E '(FAIL|panic)' | tee /dev/tty | wc -l
