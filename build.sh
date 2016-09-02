if [ ! -d bin ]; then
  mkdir bin
fi

scalac -d bin/ MatchMaker.scala