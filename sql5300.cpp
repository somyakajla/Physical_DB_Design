/**
 * @file sql5300.cpp - main entry for the relation manaager's SQL shell
 * @author Kevin Lundeen
 * @see "Seattle University, cpsc4300/5300, summer 2018"
 */
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <string>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "ParseTreeToString.h"
#include "SQLExec.h"
using namespace std;
using namespace hsql;

/*
 * we allocate and initialize the _DB_ENV global
 */
void initialize_environment(char *envHome);


/**
 * Main entry point of the sql5300 program
 * @args dbenvpath  the path to the BerkeleyDB database environment
 */
int main(int argc, char *argv[]) {

	// Open/create the db enviroment
	if (argc != 2) {
		cerr << "Usage: cpsc5300: dbenvpath" << endl;
		return 1;
	}
	initialize_environment(argv[1]);

	// Enter the SQL shell loop
	while (true) {
		cout << "SQL> ";
		string query;
		getline(cin, query);
		if (query.length() == 0)
			continue;  // blank line -- just skip
		if (query == "quit")
			break;  // only way to get out
		if (query == "test") {
			cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
			continue;
		}

		// parse and execute
		SQLParserResult* parse = SQLParser::parseSQLString(query);
		if (!parse->isValid()) {
			cout << "invalid SQL: " << query << endl;
			cout << parse->errorMsg() << endl;
		} else {
			for (uint i = 0; i < parse->size(); ++i) {
				const SQLStatement *statement = parse->getStatement(i);
				try {
					cout << ParseTreeToString::statement(statement) << endl;
					QueryResult *result = SQLExec::execute(statement);
					cout << *result << endl;
					delete result;
				} catch (SQLExecError& e) {
					cout << "Error: " << e.what() << endl;
				}
			}
		}
		delete parse;
	}
	return EXIT_SUCCESS;
}

DbEnv *_DB_ENV;
void initialize_environment(char *envHome) {
	cout << "(sql5300: running with database environment at " << envHome
		 << ")" << endl;

	DbEnv *env = new DbEnv(0U);
	env->set_message_stream(&cout);
	env->set_error_stream(&cerr);
	try {
		env->open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
	} catch (DbException &exc) {
		cerr << "(sql5300: " << exc.what() << ")" << endl;
		exit(1);
	}
	_DB_ENV = env;
	initialize_schema_tables();
}
