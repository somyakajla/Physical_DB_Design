/**
 * @file SQLExec.h - SQLExec class 
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#pragma once

#include <exception>
#include <string>
#include "SQLParser.h"
#include "schema_tables.h"

/**
 * @class SQLExecError - exception for SQLExec methods
 */
class SQLExecError : public std::runtime_error {
public:
    explicit SQLExecError(std::string s) : runtime_error(s) {}
};


/**
 * @class QueryResult - data structure to hold all the returned data for a query execution
 */
class QueryResult {
public:
    QueryResult() : column_names(nullptr), column_attributes(nullptr), rows(nullptr), message("") {}

    QueryResult(std::string message) : column_names(nullptr), column_attributes(nullptr), rows(nullptr),
                                       message(message) {}

    QueryResult(ColumnNames *column_names, ColumnAttributes *column_attributes, ValueDicts *rows, std::string message)
            : column_names(column_names), column_attributes(column_attributes), rows(rows), message(message) {}

    virtual ~QueryResult();

    ColumnNames *get_column_names() const { return column_names; }
    ColumnAttributes *get_column_attributes() const { return column_attributes; }
    ValueDicts *get_rows() const { return rows; }
    const std::string &get_message() const { return message; }
    friend std::ostream &operator<<(std::ostream &stream, const QueryResult &qres);

protected:
    ColumnNames *column_names;
    ColumnAttributes *column_attributes;
    ValueDicts *rows;
    std::string message;
};


/**
 * @class SQLExec - execution engine
 */
class SQLExec {
public:
	/**
	 * Execute the given SQL statement.  Uses one of the protected functions below
	 * @param statement   the Hyrise AST of the SQL statement to execute
	 * @returns           the query result (freed by caller)
	 */
    static QueryResult *execute(const hsql::SQLStatement *statement) throw(SQLExecError);

protected:
	// the one place in the system that holds the _tables table and _indices table
    static Tables *tables;
	static Indices *indices;

	// recursive decent into the AST: starts with create(...), drop(...) or show(...)
    // FIXME in the future will also need support for select(...), insert(...) &c...

    // Create a new Table or Index - determines which type of CreateStatement is passed
    // and calls the correct create_ function
    static QueryResult *create(const hsql::CreateStatement *statement);

    // Create a new table: adds table name to _tables, and adds columns (names + datatypes)
    // to the _columns table
    static QueryResult *create_table(const hsql::CreateStatement *statement);

    // Create a new index: adds a new row containg the index information to the _indices
    // table.  For now (FIXME) assumes a BTREE type with is_unique = true
   static QueryResult *create_index(const hsql::CreateStatement *statement);

    // Drop a table or an index: check which type of DropStatement is passed
    // and calls the correct drop_ function
    static QueryResult *drop(const hsql::DropStatement *statement);

    // Drop a table.  Table name is removed from _tables, columns from _columns
    // The coresonding file is also deleted 
    static QueryResult *drop_table(const hsql::DropStatement *statement);

    // Drop an index.  The corresponding row is removed from the _indices file
    // FIXME will need to actualy delete the index storage file once that is implemented
    static QueryResult *drop_index(const hsql::DropStatement *statement);

    // Show tables, columns, or an index.  Calls the appropriate show_ function
    static QueryResult *show(const hsql::ShowStatement *statement);

    // Get the list of tables: the contents of the _tables table
    static QueryResult *show_tables();

    // Check that a specific table exists
    // Not originally provided by Kevin - we added this for Milestone 4 in order
    // to get nicer error handling in some cases
    static bool table_exists(Identifier table_name_to_check);

    // Check that the specifed index on the specifed table exists
    // Alos not originally provided in this header file - added to make error
    // handling nicer
    static bool index_exists(Identifier table_name, Identifier index_name);

    // Get the list of columns in a specifed table: the contetns of the _columns table
    // limited to those that match a given table name
    static QueryResult *show_columns(const hsql::ShowStatement *statement);

    // Helper function for drop_index(...) deletes the rows from _indices that corespond to a
    // specifc index name
    static void delete_index_table_row(Identifier table_name, Identifier index_name);

    // Shows the indices associated with a given table
    // the rows of the _indices table where the table name is a match
    // overloaded to take statment or directly take Identifier as table name
    static QueryResult *show_index(const hsql::ShowStatement *statement);
    static QueryResult *show_index(const Identifier table_name);

	/**
	 * Pull out column name and attributes from AST's column definition clause
	 * @param col                AST column definition
	 * @param column_name        returned by reference
	 * @param column_attributes  returned by reference
	 */
    static void column_definition(const hsql::ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute);
};

