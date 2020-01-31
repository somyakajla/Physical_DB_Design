# Makefile, Kevin Lundeen, Seattle University, CPSC5300, Summer 2018
# 
CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb
COURSE      = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR     = $(COURSE)/lib

# following is a list of all the compiled object files needed to build the sql5300 executable
OBJS       = sql5300.o heap_storage.o ParseTreeToString.o SQLExec.o schema_tables.o storage_engine.o

# Rule for linking to create the executable
# Note that this is the default target since it is the first non-generic one in the Makefile: $ make
sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $(OBJS) -ldb_cxx -lsqlparser

# In addition to the general .cpp to .o rule below, we need to note any header dependencies here
# idea here is that if any of the included header files changes, we have to recompile
HEAP_STORAGE_H = heap_storage.h storage_engine.h
SCHEMA_TABLES_H = schema_tables.h $(HEAP_STORAGE_H)
SQLEXEC_H = SQLExec.h $(SCHEMA_TABLES_H)
ParseTreeToString.o : ParseTreeToString.h
SQLExec.o : $(SQLEXEC_H)
heap_storage.o : $(HEAP_STORAGE_H)
schema_tables.o : $(SCHEMA_TABLES_) ParseTreeToString.h
sql5300.o : $(SQLEXEC_H) ParseTreeToString.h
storage_engine.o : storage_engine.h

# General rule for compilation
%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"

# Rule for removing all non-source files (so they can get rebuilt from scratch)
# Note that since it is not the first target, you have to invoke it explicitly: $ make clean
clean:
	rm -f sql5300 *.o
