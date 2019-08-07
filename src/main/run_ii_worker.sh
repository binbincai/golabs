export GOPATH=/Users/caibinbin/code/golabs/6.824

a="7778 7779 77710 77711 77712 77713 77714 77715 77715"
for port in $a; do
    go run ii.go worker localhost:7777 localhost:$port &
done
