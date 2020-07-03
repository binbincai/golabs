rm *.log >/dev/null 2>&1

go test -run TestUnreliableChurn2C

if [[ $? -eq 0 ]]; then
  rm *.log >/dev/null 2>&1
fi