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
#include "sqlhelper.h"
#include "heap_storage.h"
using namespace std;
using namespace hsql;

/*
 * we allocate and initialize the _DB_ENV global
 */
DbEnv* _DB_ENV;

// forward declare
string operatorExpressionToString(const Expr* expr);

/**
 * Convert the hyrise Expr AST back into the equivalent SQL
 * @param expr expression to unparse
 * @return     SQL equivalent to *expr
 */
string expressionToString(const Expr *expr) {
	string ret;
	switch (expr->type) {
	case kExprStar:
		ret += "*";
		break;
	case kExprColumnRef:
		if (expr->table != NULL)
			ret += string(expr->table) + ".";
	case kExprLiteralString:
		ret += expr->name;
		break;
	case kExprLiteralFloat:
		ret += to_string(expr->fval);
		break;
	case kExprLiteralInt:
		ret += to_string(expr->ival);
		break;
	case kExprFunctionRef:
		ret += string(expr->name) + "?" + expr->expr->name;
		break;
	case kExprOperator:
		ret += operatorExpressionToString(expr);
		break;
	default:
		ret += "???";  // in case there are exprssion types we don't know about here
		break;
	}
	if (expr->alias != NULL)
		ret += string(" AS ") + expr->alias;
	return ret;
}

/**
 * Convert the hyrise Expr AST for an operator expression back into the equivalent SQL
 * @param expr operator expression to unparse
 * @return     SQL equivalent to *expr
 */
string operatorExpressionToString(const Expr* expr) {
	if (expr == NULL)
		return "null";

	string ret;
	// Unary prefix operator: NOT
	if(expr->opType == Expr::NOT)
		ret += "NOT ";

	// Left-hand side of expression
	ret += expressionToString(expr->expr) + " ";

	// Operator itself
	switch (expr->opType) {
	case Expr::SIMPLE_OP:
		ret += expr->opChar;
		break;
	case Expr::AND:
		ret += "AND";
		break;
	case Expr::OR:
		ret += "OR";
		break;
	default:
		break; // e.g., for NOT
	}

	// Right-hand side of expression (only present for binary operators)
	if (expr->expr2 != NULL)
		ret += " " + expressionToString(expr->expr2);
	return ret;
}

/**
 * Convert the hyrise TableRef AST back into the equivalent SQL
 * @param table  table reference AST to unparse
 * @return       SQL equivalent to *table
 */
string tableRefInfoToString(const TableRef *table) {
	string ret;
	switch (table->type) {
	case kTableSelect:
		ret += "kTableSelect FIXME"; // FIXME
		break;
	case kTableName:
		ret += table->name;
		if (table->alias != NULL)
			ret += string(" AS ") + table->alias;
		break;
	case kTableJoin:
		ret += tableRefInfoToString(table->join->left);
		switch (table->join->type) {
		case kJoinCross:
		case kJoinInner:
			ret += " JOIN ";
			break;
		case kJoinOuter:
		case kJoinLeftOuter:
		case kJoinLeft:
			ret += " LEFT JOIN ";
			break;
		case kJoinRightOuter:
		case kJoinRight:
			ret += " RIGHT JOIN ";
			break;
		case kJoinNatural:
			ret += " NATURAL JOIN ";
			break;
		}
		ret += tableRefInfoToString(table->join->right);
		if (table->join->condition != NULL)
			ret += " ON " + expressionToString(table->join->condition);
		break;
	case kTableCrossProduct:
		bool doComma = false;
		for (TableRef* tbl : *table->list) {
			if (doComma)
				ret += ", ";
			ret += tableRefInfoToString(tbl);
			doComma = true;
		}
		break;
	}
	return ret;
}

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
string columnDefinitionToString(const ColumnDefinition *col) {
	string ret(col->name);
	switch(col->type) {
	case ColumnDefinition::DOUBLE:
		ret += " DOUBLE";
		break;
	case ColumnDefinition::INT:
		ret += " INT";
		break;
	case ColumnDefinition::TEXT:
		ret += " TEXT";
		break;
	default:
		ret += " ...";
		break;
	}
	return ret;
}

/**
 * Execute an SQL select statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the select statement
 * @returns     a string (for now) of the SQL statment
 */
string executeSelect(const SelectStatement *stmt) {
	string ret("SELECT ");
	bool doComma = false;
	for (Expr* expr : *stmt->selectList) {
		if(doComma)
			ret += ", ";
		ret += expressionToString(expr);
		doComma = true;
	}
	ret += " FROM " + tableRefInfoToString(stmt->fromTable);
	if (stmt->whereClause != NULL)
		ret += " WHERE " + expressionToString(stmt->whereClause);
	return ret;
}

/**
 * Execute an SQL insert statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the insert statement
 * @returns     a string (for now) of the SQL statment
 */
string executeInsert(const InsertStatement *stmt) {
	return "INSERT ...";
}

/**
 * Execute an SQL create statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the create statement
 * @returns     a string (for now) of the SQL statment
 */
string executeCreate(const CreateStatement *stmt) {
	string ret("CREATE TABLE ");
	if (stmt->type != CreateStatement::kTable )
		return ret + "...";
	if (stmt->ifNotExists)
		ret += "IF NOT EXISTS ";
	ret += string(stmt->tableName) + " (";
	bool doComma = false;
	for (ColumnDefinition *col : *stmt->columns) {
		if(doComma)
			ret += ", ";
		ret += columnDefinitionToString(col);
		doComma = true;
	}
	ret += ")";
	return ret;
}

/**
 * Execute an SQL statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the statement
 * @returns     a string (for now) of the SQL statment
 */
string execute(const SQLStatement *stmt) {
	switch (stmt->type()) {
	case kStmtSelect:
		return executeSelect((const SelectStatement*) stmt);
	case kStmtInsert:
		return executeInsert((const InsertStatement*) stmt);
	case kStmtCreate:
		return executeCreate((const CreateStatement*) stmt);
	default:
		return "Not implemented";
	}
}

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
	char *envHome = argv[1];
	cout << "(sql5300: running with database environment at " << envHome << ")" << endl;
	DbEnv env(0U);
	env.set_message_stream(&cout);
	env.set_error_stream(&cerr);
	try {
		env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
	} catch (DbException& exc) {
		cerr << "(sql5300: " << exc.what() << ")";
		exit(1);
	}
	_DB_ENV = &env;

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

		// use the Hyrise sql parser to get us our AST
		SQLParserResult* result = SQLParser::parseSQLString(query);
		if (!result->isValid()) {
			cout << "invalid SQL: " << query << endl;
			delete result;
			continue;
		}

		// execute the statement
		for (uint i = 0; i < result->size(); ++i) {
			cout << execute(result->getStatement(i)) << endl;
		}
		delete result;
	}
	return EXIT_SUCCESS;
}

