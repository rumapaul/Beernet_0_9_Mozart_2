MAKE = make
INSTALL = install
INSTALL_FUNCTOR = $(INSTALL) -m 644
DESTDIR = ../../lib/network/
SRCS = FailureDetector Network Pbeerp2p Pp2p 

all: $(foreach source, $(SRCS), src_$(source))

install: check_destdir $(foreach source, $(SRCS), install_$(source))

check_destdir:
	if ! test -d $(DESTDIR); then mkdir $(DESTDIR); fi

install_%:
	$(INSTALL_FUNCTOR) $(subst install_,,$@).ozf $(DESTDIR)

src_%:
	ozc -c $(subst src_,,$@).oz -o $(subst src_,,$@).ozf

alltests: tester testerdist testerfaildetector

tester: Tester.oz TestPlayers.oz
	ozc -c TestPlayers.oz -o TestPlayers.ozf
	ozc -x Tester.oz -o tester

testerdist: tester TesterDistNode.oz TesterDist.oz
	ozc -x TesterDistNode.oz -o testerDistNode
	ozc -x TesterDist.oz -o testerDist

testerfaildetector: TestFailDetNode.oz TestFailDet.oz
	ozc -x TestFailDetNode.oz -o testFailDetNode
	ozc -x TestFailDet.oz -o testFailDet

clean:
	rm -f *ozf
	rm -f tester
	rm -f testerDist
	rm -f testerDistNode
	rm -f testFailDet
	rm -f testFailDetNode
	rm -f coordinator.tket

.PHONY: all clean
