#!/usr/bin/env sh

go test -fuzz=FuzzLessThan_ -fuzztime 5m
go test -fuzz=FuzzLessThanOrEqualTo_ -fuzztime 5m
go test -fuzz=FuzzGreaterThan_ -fuzztime 5m
go test -fuzz=FuzzGreaterThanOrEqualTo_ -fuzztime 5m
go test -fuzz=FuzzEqual_ -fuzztime 5m
go test -fuzz=FuzzContain_ -fuzztime 5m
go test -fuzz=FuzzContainElement_ -fuzztime 5m
go test -fuzz=FuzzOverlap_ -fuzztime 5m
go test -fuzz=FuzzLeftOf_ -fuzztime 5m
go test -fuzz=FuzzRightOf_ -fuzztime 5m
go test -fuzz=FuzzAdjacent_ -fuzztime 5m
go test -fuzz=FuzzIntersect_ -fuzztime 5m
go test -fuzz=FuzzNotExtendRight_ -fuzztime 5m
go test -fuzz=FuzzNotExtendLeft_ -fuzztime 5m
go test -fuzz=FuzzUnion_ -fuzztime 5m
go test -fuzz=FuzzMerge_ -fuzztime 5m
go test -fuzz=FuzzDifference_ -fuzztime 5m
