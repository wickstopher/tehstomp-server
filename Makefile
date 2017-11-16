build:
	cabal build
	ghc StompBroker

docs:
	cabal haddock

install: build docs
	cabal install --enable-documentation

clean:
	rm StompBroker *.o *.hi Stomp/*.o Stomp/*.hi
	rm -rf dist/

uninstall: clean
	ghc-pkg unregister tehstomp-server

reinstall: uninstall install
