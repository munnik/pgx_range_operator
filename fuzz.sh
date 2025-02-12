#!/usr/bin/env sh

go test -fuzz=FuzzLessThan$ -fuzztime 5s
go test -fuzz=FuzzLessThanOrEqualTo$ -fuzztime 5s
go test -fuzz=FuzzGreaterThan$ -fuzztime 5s
go test -fuzz=FuzzGreaterThanOrEqualTo$ -fuzztime 5s
go test -fuzz=FuzzEqual$ -fuzztime 5s
go test -fuzz=FuzzContain$ -fuzztime 5s
go test -fuzz=FuzzContainElement$ -fuzztime 5s
go test -fuzz=FuzzOverlap$ -fuzztime 5s
go test -fuzz=FuzzLeftOf$ -fuzztime 5s
go test -fuzz=FuzzRightOf$ -fuzztime 5s
go test -fuzz=FuzzAdjacent$ -fuzztime 5s
go test -fuzz=FuzzIntersect$ -fuzztime 5s
go test -fuzz=FuzzNotExtendRight$ -fuzztime 5s
go test -fuzz=FuzzNotExtendLeft$ -fuzztime 5s
go test -fuzz=FuzzUnion$ -fuzztime 5s
go test -fuzz=FuzzMerge$ -fuzztime 5s
go test -fuzz=FuzzDifference$ -fuzztime 5s
