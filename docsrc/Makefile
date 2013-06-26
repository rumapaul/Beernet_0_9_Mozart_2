MAKE = make
LATEX = pdflatex
DIATOPDF = diatopdf
INSTALL = install
INSTALL_DATA = $(INSTALL) -m 644
DESTDIR = ../doc/
DOCS = changelog architecture-detail.pdf architecture-general.pdf api.pdf

all: pdf diagrams

install: check_destdir $(foreach doc, $(DOCS), install_$(doc))

check_destdir:
	if ! test -d $(DESTDIR); then mkdir $(DESTDIR); fi

install_%:
	$(INSTALL_DATA) $(subst install_,,$@) $(DESTDIR)

pdf:
	$(LATEX) api.tex
	$(LATEX) api.tex

diagrams:
	$(DIATOPDF) architecture-detail.dia
	$(DIATOPDF) architecture-general.dia

clean:
	rm -f *.aux
	rm -f *.dvi
	rm -f *.log
	rm -f *.out
	rm -f *~

.PHONY: all clean
