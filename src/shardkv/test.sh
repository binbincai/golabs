set -x
rm *.log >/dev/null 2>&1
go test -run TestConcurrent1 -race
if [[ $? -eq 0 ]]; then
  rm *.log >/dev/null 2>&1
fi

# 44xwqCF2xXBCwzxeXtKNp6gaeNrGhxM6uyIqafg552TVqs8YfL9uJ7AiTOGlA4a0LPHslzSZVErk6APSIPHasFIssq29r6PACyyfD1r9SYuorCkHfExCx_eQUgELvzd2VG4QrrV 5eD0T o8HtA H88mfX2iSXBymgnVxShX
# 44xwqCF2xXBCwzxeXtKNp6gaeNrGhxM6uyIqafg552TVqs8YfL9uJ7AiTOGlA4a0LPHslzSZVErk6APSIPHasFIssq29r6PACyyfD1r9SYuorCkHfExCx_eQUgELvzd2VG4QrrV 5eD0T       H88mfX2iSXBymgnVxShX