build:
	ghc StompBroker -threaded +RTS -N4

docs:
	cabal configure
	cabal haddock

install: docs
	cabal install --enable-documentation

clean:
	rm -f StompBroker *.o *.hi *.log Stomp/*.o Stomp/*.hi
	rm -rf dist/

uninstall: clean
	ghc-pkg unregister tehstomp-server

reinstall: uninstall install
