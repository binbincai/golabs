set -x
rm *.log >/dev/null 2>&1
go test -run TestConcurrent2 #-race
if [[ $? -eq 0 ]]; then
  rm *.log >/dev/null 2>&1
fi