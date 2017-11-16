build:
	ghc StompBroker

docs:
	cabal configure
	cabal haddock

install: docs
	cabal install --enable-documentation

clean:
	rm -f StompBroker *.o *.hi Stomp/*.o Stomp/*.hi
	rm -rf dist/

uninstall: clean
	ghc-pkg unregister tehstomp-server

reinstall: uninstall install
